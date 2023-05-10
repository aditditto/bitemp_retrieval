MODULE_big = bitemp_retrieval
EXTENSION = bitemp_retrieval
DATA = bitemp_retrieval--0.0.1.sql
DOCS = README.bitemp_retrieval

OBJS = 	src/bitemp_retrieval.o \
		src/test.o

REGRESS = bitemp_time_contains_test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)