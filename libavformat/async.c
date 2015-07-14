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
 * Based on libavformat/cache.c by Michael Niedermayer
 */

 /**
 * @TODO
 *      support timeout
 *      support backward short seek
 *      support work with concatdec, hls
 */

#include "libavutil/avassert.h"
#include "libavutil/avstring.h"
#include "libavutil/error.h"
#include "libavutil/fifo.h"
#include "libavutil/log.h"
#include "libavutil/opt.h"
#include "url.h"
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>

#define BUFFER_CAPACITY         (4 * 1024 * 1024)
#define SHORT_SEEK_THRESHOLD    (256 * 1024)

/* FIXME: remove before submit patch */
#define AVTRACE av_log

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
    size_t          logical_size;
    AVFifoBuffer   *fifo;

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
    URLContext   *h    = arg;
    Context      *c    = h->priv_data;
    AVFifoBuffer *fifo = c->fifo;
    int           ret  = 0;

    while (1) {
        if (async_interrupt_callback(h)) {
            c->io_eof_reached = 1;
            c->io_error       = AVERROR_EXIT;
            break;
        }

        if (c->seek_request) {
            pthread_mutex_lock(&c->mutex);

            ret = ffurl_seek(c->inner, c->seek_pos, c->seek_whence);
            if (ret < 0) {
                c->io_eof_reached = 1;
                c->io_error       = ret;
            } else {
                c->io_eof_reached = 0;
                c->io_error       = 0;
            }

            c->seek_completed = 1;
            c->seek_ret       = ret;
            c->seek_request   = 0;

            av_fifo_reset(fifo);

            pthread_cond_signal(&c->cond_wakeup_main);
            pthread_mutex_unlock(&c->mutex);
            continue;
        }

        if (c->io_eof_reached || av_fifo_space(fifo) <= 0) {
            pthread_mutex_lock(&c->mutex);
            pthread_cond_signal(&c->cond_wakeup_main);
            pthread_cond_wait(&c->cond_wakeup_background, &c->mutex);
            pthread_mutex_unlock(&c->mutex);
            continue;
        }

        ret = av_fifo_generic_write(fifo, c->inner, 65535, (void *)ffurl_read);
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
    Context         *c = h->priv_data;
    int              ret;
    AVIOInterruptCB  interrupt_callback = {.callback = async_interrupt_callback, .opaque = h};

    av_strstart(arg, "async:", &arg);

    c->fifo = av_fifo_alloc(BUFFER_CAPACITY);
    if (!c->fifo) {
        ret = AVERROR(ENOMEM);
        goto fifo_fail;
    }

    /* wrap interrupt callback */
    c->interrupt_callback = h->interrupt_callback;
    ret = ffurl_open(&c->inner, arg, flags, &interrupt_callback, options);
    if (ret != 0) {
        av_log(h, AV_LOG_ERROR, "ffurl_open failed : %s, %s\n", strerror(ret), arg);
        goto url_fail;
    }

    c->logical_size = ffurl_size(c->inner);
    h->is_streamed  = c->inner->is_streamed;

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
cond_wakeup_background_fail:
    pthread_cond_destroy(&c->cond_wakeup_main);
cond_wakeup_main_fail:
    pthread_mutex_destroy(&c->mutex);
mutex_fail:
    ffurl_close(c->inner);
url_fail:
    av_fifo_freep(&c->fifo);
fifo_fail:
    return ret;
}

static int async_close(URLContext *h)
{
    Context *c = h->priv_data;
    int      ret;

    pthread_mutex_lock(&c->mutex);
    c->abort_request = 1;
    pthread_cond_signal(&c->cond_wakeup_background);
    pthread_mutex_unlock(&c->mutex);

    ret = pthread_join(c->async_buffer_thread, NULL);
    if (ret != 0)
        av_log(h, AV_LOG_ERROR, "pthread_join(): %s\n", strerror(ret));

    pthread_cond_destroy(&c->cond_wakeup_background);
    pthread_cond_destroy(&c->cond_wakeup_main);
    pthread_mutex_destroy(&c->mutex);
    ffurl_close(c->inner);
    av_fifo_freep(&c->fifo);

    return 0;
}

static int async_read_internal(URLContext *h, void *dest, int size, int read_complete,
                               void (*func)(void*, void*, int))
{
    Context      *c       = h->priv_data;
    AVFifoBuffer *fifo    = c->fifo;
    int           to_read = size;
    int           ret     = 0;

    pthread_mutex_lock(&c->mutex);

    while (to_read > 0) {
        if (async_interrupt_callback(h)) {
            ret = AVERROR_EXIT;
            break;
        }
        int fifo_size = av_fifo_size(fifo);
        int to_copy   = FFMIN(to_read, fifo_size);
        if (to_copy > 0) {
            av_fifo_generic_read(fifo, dest, to_copy, NULL);
            dest            = (uint8_t *)dest + to_copy;
            c->logical_pos += to_copy;
            to_read        -= to_copy;
            ret             = size - to_read;

            if (to_read <= 0 || !read_complete)
                break;
        } else if (c->io_eof_reached) {
            ret = AVERROR_EOF;
            break;
        }
        pthread_cond_signal(&c->cond_wakeup_background);
        pthread_cond_wait(&c->cond_wakeup_main, &c->mutex);
    }

    pthread_cond_signal(&c->cond_wakeup_background);
    pthread_mutex_unlock(&c->mutex);

    return ret;
}

static int async_read(URLContext *h, unsigned char *buf, int size)
{
    Context *c = h->priv_data;
    int old_pos = (int)c->logical_pos;
    int ret = async_read_internal(h, buf, size, 0, NULL);
    AVTRACE(h, AV_LOG_DEBUG, "async_read(%d)=%d at %d\n", size, ret, old_pos);

    return ret;
}

static void fifo_do_not_copy_func(void* dest, void* src, int size) {
    // do not copy
}

static int64_t async_seek(URLContext *h, int64_t pos, int whence)
{
    Context      *c    = h->priv_data;
    AVFifoBuffer *fifo = c->fifo;
    int64_t       ret;
    int64_t       new_logical_pos;

    if (whence == AVSEEK_SIZE) {
        AVTRACE(h, AV_LOG_DEBUG, "async_seek: AVSEEK_SIZE\n");
        return c->logical_size;
    } if (whence == SEEK_CUR) {
        AVTRACE(h, AV_LOG_DEBUG, "async_seek: %"PRId64"\n", pos);
        new_logical_pos = pos + c->logical_pos;
    } else if (whence == SEEK_SET){
        AVTRACE(h, AV_LOG_DEBUG, "async_seek: %"PRId64"\n", pos);
        new_logical_pos = pos;
    } else {
        return AVERROR(EINVAL);
    }
    if (new_logical_pos < 0)
        return AVERROR(EINVAL);

    int fifo_size = av_fifo_size(fifo);
    if (new_logical_pos == c->logical_pos) {
        /* current position */
        return c->logical_pos;
    } else if ((new_logical_pos > c->logical_pos) &&
               (new_logical_pos < (c->logical_pos + fifo_size + SHORT_SEEK_THRESHOLD))) {
        /* fast seek */
        AVTRACE(h, AV_LOG_DEBUG, "async_seek: fask_seek %"PRId64" from %d dist:%d/%d\n",
                new_logical_pos, (int)c->logical_pos,
                (int)(new_logical_pos - c->logical_pos), fifo_size);
        async_read_internal(h, NULL, new_logical_pos - c->logical_pos, 1, fifo_do_not_copy_func);
        return c->logical_pos;
    } else if (c->logical_size <= 0) {
        /* can not seek */
        return AVERROR(EINVAL);
    } else if (new_logical_pos > c->logical_size) {
        /* beyond end */
        return AVERROR(EINVAL);
    }

    pthread_mutex_lock(&c->mutex);

    c->seek_request   = 1;
    c->seek_pos       = new_logical_pos;
    c->seek_whence    = SEEK_SET;
    c->seek_completed = 0;
    c->seek_ret       = 0;

    while (1) {
        if (async_interrupt_callback(h)) {
            ret = AVERROR_EXIT;
            break;
        }
        if (c->seek_completed) {
            if (c->seek_ret >= 0)
                c->logical_pos  = c->seek_ret;
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
