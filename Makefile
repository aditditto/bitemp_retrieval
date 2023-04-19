MODULE_big = bitemp-retrieval
EXTENSION = bitemp-retrieval
DATA = bitemp-retrieval--0.0.1.sql
DOCS = README.bitemp-retrieval

OBJS = 	src/bitemp-retrieval.o \
		src/test.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)