# libavcodec tests
AVCODECOBJS-$(CONFIG_BSWAPDSP) += bswapdsp.o
AVCODECOBJS-$(CONFIG_H264PRED) += h264pred.o
AVCODECOBJS-$(CONFIG_H264QPEL) += h264qpel.o

CHECKASMOBJS-$(CONFIG_AVCODEC) += $(AVCODECOBJS-yes)


-include $(SRC_PATH)/tests/checkasm/$(ARCH)/Makefile

CHECKASMOBJS += $(CHECKASMOBJS-yes) checkasm.o
CHECKASMOBJS := $(sort $(CHECKASMOBJS:%=tests/checkasm/%))

-include $(CHECKASMOBJS:.o=.d)

CHECKASMDIRS := $(sort $(dir $(CHECKASMOBJS)))
$(CHECKASMOBJS): | $(CHECKASMDIRS)
OBJDIRS += $(CHECKASMDIRS)

# We rely on function pointers intentionally declared without specified argument types.
tests/checkasm/%.o: CFLAGS := $(CFLAGS:-Wstrict-prototypes=-Wno-strict-prototypes)

CHECKASM := tests/checkasm/checkasm$(EXESUF)

$(CHECKASM): $(EXEOBJS) $(CHECKASMOBJS) $(FF_DEP_LIBS)
	$(LD) $(LDFLAGS) $(LDEXEFLAGS) $(LD_O) $(CHECKASMOBJS) $(FF_EXTRALIBS)

checkasm: $(CHECKASM)

clean:: checkasmclean

checkasmclean:
	$(RM) $(CHECKASM) $(CLEANSUFFIXES:%=tests/checkasm/%) $(CLEANSUFFIXES:%=tests/checkasm/$(ARCH)/%)

.PHONY: checkasm
