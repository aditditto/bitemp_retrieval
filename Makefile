MODULE_big = bitemp_retrieval
EXTENSION = bitemp_retrieval
DATA = bitemp_retrieval--0.0.1.sql
DOCS = README.bitemp_retrieval

OBJS = 	src/bitemp_retrieval.o \
		src/test.o

# Order is important, first test needs to install pg_bitemporal
REGRESS = _install_pg_bitemporal \
		  bitemp_contains_timeslice_test \
		  bitemp_join_functions_test

srcdir = $(pwd)

bitemp_regress_dir = $(srcdir)/regress

REGRESS_OPTS = --inputdir=$(bitemp_regress_dir) --outputdir=$(bitemp_regress_dir)		  

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

clean:
	rm -f bitemp_retrieval.so   libbitemp_retrieval.a  libbitemp_retrieval.pc
	rm -f src/bitemp_retrieval.o src/test.o src/bitemp_retrieval.bc src/test.bc
	rm -rf $(bitemp_regress_dir)/results/ $(bitemp_regress_dir)/regression.diffs $(bitemp_regress_dir)/regression.out tmp_check/ tmp_check_iso/ log/ output_iso/

adit: clean uninstall install installcheck