/*
 * Input async protocol.
 * Copyright (c) 2015 Zhang Rui <bbcallen@gmail.com>
 *
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 * Based on file.c by Fabrice Bellard
 */

#include "libavutil/avstring.h"
#include "libavutil/error.h"
#include "libavutil/log.h"
#include "libavutil/opt.h"
#include "url.h"
#include <stdint.h>
#include <pthread.h>

#define BUFFER_CAPACITY (4 * 1024)

typedef struct RingBuffer {
    size_t          capacity;
    uint8_t        *buffer_begin;
    uint8_t        *buffer_end;

    size_t          size;
    uint8_t        *write_pointer;
    uint8_t        *read_pointer;
} RingBuffer;

static int ring_init(RingBuffer *ring)
{
    memset(ring, 0, sizeof(RingBuffer));
    ring->capacity      = BUFFER_CAPACITY;

    ring->buffer_begin = av_malloc(BUFFER_CAPACITY);
    if (!ring->buffer_begin)
        return AVERROR(ENOMEM);

    ring->buffer_end    = ring->buffer_begin + ring->capacity;
    ring->write_pointer = ring->buffer_begin;
    ring->read_pointer  = ring->buffer_end;
    return 0;
}

static void ring_destroy(RingBuffer *ring)
{
    if (ring->buffer_begin)
        av_free(ring->buffer_begin);
    memset(ring, 0, sizeof(RingBuffer));
}

static int ring_space(RingBuffer *ring)
{
    return ring->capacity - ring->size;
}

static int ring_drain(RingBuffer *ring, URLContext *inner, size_t bytes)
{
    int    ret      = 0;
    size_t to_drain = bytes;

    while (to_drain > 0) {
        int space = ring_space(ring);
        if (space <= 0)
            break;

        if (ring->write_pointer >= ring->buffer_end)
            ring->write_pointer = ring->buffer_begin;

        int to_read = (int)FFMIN3(to_drain, space, (ring->buffer_end - ring->write_pointer));
        ret = ffurl_read(inner, ring->write_pointer, to_read);
        if (ret < 0)
            return ret;

        to_drain            -= ret;
        ring->write_pointer += ret;
        ring->size          += ret;
    }

    return bytes - to_drain;
}

static int ring_read(RingBuffer *ring, uint8_t *buf, size_t bytes)
{
    size_t to_consume = bytes;

    while (to_consume > 0) {
        int space = ring_space(ring);
        if (space <= 0)
            break;

        if (ring->read_pointer >= ring->buffer_end)
            ring->read_pointer = ring->buffer_begin;

        int to_copy = (int)FFMIN3(to_consume, space, (ring->buffer_end - ring->read_pointer));
        memcpy(buf, ring->read_pointer, to_copy);

        to_consume         -= to_copy;
        ring->read_pointer += to_copy;
        ring->size         -= to_copy;
    }

    return bytes - to_consume;
}

static void ring_reset(RingBuffer *ring)
{
    ring->write_pointer = ring->buffer_begin;
    ring->read_pointer  = ring->buffer_begin;
    ring->size          = 0;
}

typedef struct Context {
    AVClass        *class;
    URLContext     *inner;

    int             seek_request;
    size_t          seek_pos;
    int             seek_whence;
    int             seek_completed;
    int64_t         seek_ret;

    int             io_error;
    int             io_eof_reached;

    size_t          logical_pos;
    RingBuffer      ring_buffer;

    pthread_cond_t  cond_wakeup_main;
    pthread_cond_t  cond_wakeup_background;
    pthread_mutex_t mutex;
    pthread_t       async_buffer_thread;

    int             abort_request;
    AVIOInterruptCB interrupt_callback;
} Context;

static int async_interrupt_callback(void *arg)
{
    URLContext *h   = arg;
    Context    *c   = h->priv_data;
    int         ret = 0;

    if (c->interrupt_callback.callback) {
        ret = c->interrupt_callback.callback(c->interrupt_callback.opaque);
        if (!ret) 
            return ret;
    }

    return c->abort_request;
}

static void *async_buffer_task(void *arg)
{
    URLContext *h    = arg;
    Context    *c    = h->priv_data;
    RingBuffer *ring = &c->ring_buffer;
    int         ret  = 0;

    while (1) {            
        if (async_interrupt_callback(h)) {
            c->io_eof_reached = 1;
            c->io_error       = AVERROR_EXIT;
            break;
        }

        if (c->io_eof_reached || (ring->size >= ring->capacity && !c->seek_request)) {
            pthread_mutex_lock(&c->mutex);
            pthread_cond_signal(&c->cond_wakeup_main);
            pthread_cond_wait(&c->cond_wakeup_background, &c->mutex);
            pthread_mutex_unlock(&c->mutex);
            continue;
        }

        if (c->seek_request) {
            pthread_mutex_lock(&c->mutex);

            /* TODO: fast-seek */

            ret = ffurl_seek(c->inner, c->seek_pos, c->seek_whence);
            if (ret < 0) {
                c->io_eof_reached = 1;
                c->io_error = ret;
            }

            c->seek_completed = 1;
            c->seek_ret       = ret;
            c->seek_request   = 0;

            ring_reset(ring);

            pthread_cond_signal(&c->cond_wakeup_main);
            pthread_mutex_unlock(&c->mutex);
        }

        ret = ring_drain(ring, c->inner, 4096);
        if (ret <= 0) {
            c->io_eof_reached = 1;
            if (ret < 0) {
                c->io_error = ret;
            }
        }

        pthread_mutex_lock(&c->mutex);
        pthread_cond_signal(&c->cond_wakeup_main);
        pthread_mutex_unlock(&c->mutex);
    }

    return NULL;
}

static int async_open(URLContext *h, const char *arg, int flags, AVDictionary **options)
{
    int ret;
    Context *c = h->priv_data;
    AVIOInterruptCB interrupt_callback = {.callback = async_interrupt_callback, .opaque = h};

    av_strstart(arg, "async:", &arg);

    ret = ring_init(&c->ring_buffer);
    if (ret != 0) {
        ret = AVERROR(ENOMEM);
        goto ring_fail;
    }

    /* wrap interrupt callback */
    c->interrupt_callback = h->interrupt_callback;
    ret = ffurl_open(&c->inner, arg, flags, &interrupt_callback, options);
    if (ret != 0) {
        av_log(h, AV_LOG_ERROR, "ffurl_open failed : %s, %s\n", strerror(ret), arg);
        goto url_fail;
    }

    ret = pthread_mutex_init(&c->mutex, NULL);
    if (ret != 0) {
        av_log(h, AV_LOG_ERROR, "pthread_mutex_init failed : %s\n", strerror(ret));
        goto mutex_fail;
    }

    ret = pthread_cond_init(&c->cond_wakeup_main, NULL);
    if (ret != 0) {
        av_log(h, AV_LOG_ERROR, "pthread_cond_init failed : %s\n", strerror(ret));
        goto cond_wakeup_main_fail;
    }

    ret = pthread_cond_init(&c->cond_wakeup_background, NULL);
    if (ret != 0) {
        av_log(h, AV_LOG_ERROR, "pthread_cond_init failed : %s\n", strerror(ret));
        goto cond_wakeup_background_fail;
    }

    ret = pthread_create(&c->async_buffer_thread, NULL, async_buffer_task, h);
    if (ret) {
        av_log(h, AV_LOG_ERROR, "pthread_create failed : %s\n", strerror(ret));
        goto thread_fail;
    }

    return 0;

thread_fail:
    pthread_cond_destroy(&c->cond_wakeup_background);
cond_wakeup_main_fail:
    pthread_cond_destroy(&c->cond_wakeup_main);
cond_wakeup_background_fail:
    pthread_mutex_destroy(&c->mutex);
mutex_fail:
    ffurl_close(c->inner);
url_fail:
    ring_destroy(&c->ring_buffer);
ring_fail:
    return ret;
}

static int async_close(URLContext *h)
{
    Context *c = h->priv_data;
    int      ret;

    ret = pthread_join(c->async_buffer_thread, NULL);
    if (ret != 0)
        av_log(h, AV_LOG_ERROR, "pthread_join(): %s\n", strerror(ret));

    pthread_cond_destroy(&c->cond_wakeup_background);
    pthread_cond_destroy(&c->cond_wakeup_main);
    pthread_mutex_destroy(&c->mutex);
    ffurl_close(c->inner);
    ring_destroy(&c->ring_buffer);

    return 0;
}

static int async_read(URLContext *h, unsigned char *buf, int size)
{
    Context    *c    = h->priv_data;
    RingBuffer *ring = &c->ring_buffer;
    int64_t     ret;

    size = FFMIN(size, ring->capacity);

    pthread_mutex_lock(&c->mutex);

    while (1) {
        if (async_interrupt_callback(h)) {
            ret = AVERROR_EXIT;
            break;
        }
        if (ring->size >= size || c->io_eof_reached) {
            ret = ring_read(ring, buf, size);
            if (ret == 0 && c->io_eof_reached)
                ret = AVERROR_EOF;
            break;
        }
        pthread_cond_signal(&c->cond_wakeup_background);
        pthread_cond_wait(&c->cond_wakeup_main, &c->mutex);
    }

    pthread_mutex_unlock(&c->mutex);

    return ret;
}

static int64_t async_seek(URLContext *h, int64_t pos, int whence)
{
    Context *c = h->priv_data;
    int64_t ret;

    pthread_mutex_lock(&c->mutex);

    c->seek_request   = 1;
    c->seek_pos       = pos;
    c->seek_whence    = whence;
    c->seek_completed = 0;
    c->seek_ret       = 0;

    while (1) {
        if (async_interrupt_callback(h)) {
            ret = AVERROR_EXIT;
            break;
        }
        if (c->seek_completed) {
            ret = c->seek_ret;
            break;
        }
        pthread_cond_signal(&c->cond_wakeup_background);
        pthread_cond_wait(&c->cond_wakeup_main, &c->mutex);
    }

    pthread_mutex_unlock(&c->mutex);

    return ret;
}

#define OFFSET(x) offsetof(Context, x)
#define D AV_OPT_FLAG_DECODING_PARAM

static const AVOption options[] = {
    //{ "read_ahead_limit", "Amount in bytes that may be read ahead when seeking isn't supported, -1 for unlimited", OFFSET(read_ahead_limit), AV_OPT_TYPE_INT, { .i64 = 65536 }, -1, INT_MAX, D },
    {NULL},
};

static const AVClass async_context_class = {
    .class_name = "Async",
    .item_name  = av_default_item_name,
    .option     = options,
    .version    = LIBAVUTIL_VERSION_INT,
};

URLProtocol ff_async_protocol = {
    .name                = "async",
    .url_open2           = async_open,
    .url_read            = async_read,
    .url_seek            = async_seek,
    .url_close           = async_close,
    .priv_data_size      = sizeof(Context),
    .priv_data_class     = &async_context_class,
};
