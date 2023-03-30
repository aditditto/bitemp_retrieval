MODULES = bitemp-retrieval
EXTENSION = bitemp-retrieval
DATA = bitemp-retrieval--0.0.1.sql
DOCS = README.bitemp-retrieval

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)