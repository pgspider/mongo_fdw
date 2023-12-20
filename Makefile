# mongo_fdw/Makefile.meta
#
# Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
# Portions Copyright © 2012–2014 Citus Data, Inc.
# Portions Copyright (c) 2021, TOSHIBA CORPORATION
#

MODULE_big = mongo_fdw

#
# We assume we are running on a POSIX compliant system (Linux, OSX). If you are
# on another platform, change env_posix.os in MONGO_OBJS with the appropriate
# environment object file.
#
LIBJSON = json-c
LIBJSON_OBJS =  $(LIBJSON)/json_util.o $(LIBJSON)/json_object.o $(LIBJSON)/json_tokener.o \
                                $(LIBJSON)/json_object_iterator.o $(LIBJSON)/printbuf.o $(LIBJSON)/linkhash.o \
                                $(LIBJSON)/arraylist.o $(LIBJSON)/random_seed.o $(LIBJSON)/debug.o $(LIBJSON)/strerror_override.o

MONGO_INCLUDE = $(shell pkg-config --cflags libmongoc-1.0)
PG_CPPFLAGS = --std=c99 $(MONGO_INCLUDE) -I$(LIBJSON) -DMETA_DRIVER
SHLIB_LINK = $(shell pkg-config --libs libmongoc-1.0)

OBJS = connection.o option.o mongo_wrapper_meta.o mongo_fdw.o mongo_query.o deparse.o $(LIBJSON_OBJS)


EXTENSION = mongo_fdw
DATA = mongo_fdw--1.0.sql  mongo_fdw--1.1.sql mongo_fdw--1.0--1.1.sql

REGRESS = server_options connection_validation dml select pushdown column_remapping join_pushdown extra/aggregates extra/join extra/json extra/jsonb extra/limit extra/enhance
REGRESS_OPTS = --load-extension=$(EXTENSION)

#
# Users need to specify their Postgres installation path through pg_config. For
# example: /usr/local/pgsql/bin/pg_config or /usr/lib/postgresql/9.1/bin/pg_config
#

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/mongo_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifndef MAJORVERSION
    MAJORVERSION := $(basename $(VERSION))
endif

ifeq (,$(findstring $(MAJORVERSION), 13 14 15 16))
    $(error PostgreSQL 13, 14 15 or 16 is required to compile this extension)
endif

ifdef REGRESS_PREFIX
REGRESS_PREFIX_SUB = $(REGRESS_PREFIX)
else
REGRESS_PREFIX_SUB = $(VERSION)
endif

REGRESS := $(addprefix $(REGRESS_PREFIX_SUB)/,$(REGRESS))
$(shell mkdir -p results/$(REGRESS_PREFIX_SUB)/extra)
