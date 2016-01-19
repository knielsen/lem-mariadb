CC             ?= gcc -std=gnu99
CFLAGS         ?= -O2 -pipe -Wall -Wextra -Wno-strict-aliasing -fno-strict-aliasing
PKG_CONFIG     ?= pkg-config
MARIADB_CONFIG ?= mariadb_config
STRIP          ?= strip
INSTALL        ?= install
UNAME          ?= uname

OS              = $(shell $(UNAME))
EXTRA_CFLAGS    = $(shell $(PKG_CONFIG) --cflags lem)
EXTRA_CFLAGS   += $(shell $(MARIADB_CONFIG) --include)
LIBS            = $(shell $(MARIADB_CONFIG) --libs)
lmoddir         = $(shell $(PKG_CONFIG) --variable=INSTALL_LMOD lem)
cmoddir         = $(shell $(PKG_CONFIG) --variable=INSTALL_CMOD lem)

ifeq ($(OS),Darwin)
SHARED          = -dynamiclib -Wl,-undefined,dynamic_lookup
STRIP           := $(STRIP) -x
else
SHARED          = -shared
endif

llibs = lem/mariadb/queued.lua
clibs = lem/mariadb.so

ifdef V
E=@\#
Q=
else
E=@echo
Q=@
endif

.PHONY: all debug strip install clean

all: EXTRA_CFLAGS += -DNDEBUG
all: $(clibs)

debug: $(clibs)

lem/mariadb.so: lem/mariadb.c
	$E '  CCLD  $@'
	$Q$(CC) $(EXTRA_CFLAGS) $(CFLAGS) -fPIC -nostartfiles $(SHARED) $^ -o $@ $(LDFLAGS) $(LIBS)

%-strip: %
	$E '  STRIP $<'
	$Q$(STRIP) $<

strip: $(clibs:%=%-strip)

$(DESTDIR)$(lmoddir)/% $(DESTDIR)$(cmoddir)/%: %
	$E '  INSTALL $@'
	$Q$(INSTALL) -d $(dir $@)
	$Q$(INSTALL) -m 644 $< $@

install: \
	$(llibs:%=$(DESTDIR)$(lmoddir)/%) \
	$(clibs:%=$(DESTDIR)$(cmoddir)/%)

clean:
	rm -f $(clibs)
