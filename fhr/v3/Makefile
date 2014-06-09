

DEST = $(HOME)/prod/ETL-frontoffice-v3/
# changes to DEST will need manual changes to SCRIPTS_PATH in run.sh

JOBS = profile_age_buckets_job.py profile_age_job.py profile_counts_job.py search_counts_job.py
LIB_ARCHIVE = lib.zip

LIB_FILES = base_etl_job.py util.py fhrdata.py
VERTICA_FILES = vertica_table_map.txt load_into_vertica.sh
RUN_FILES = run.sh

lib.zip: $(LIB_FILES)
	zip lib.zip $(LIB_FILES)

build: lib.zip $(LIB_FILES) $(VERTICA_FILES) $(RUN_FILES) $(JOBS)
	rm -fr build
	mkdir build
	cp lib.zip $(LIB_FILES) $(VERTICA_FILES) $(RUN_FILES) $(JOBS) build/

install: build
	mkdir -p $(DEST)
	cp -ra build/* $(DEST)/
