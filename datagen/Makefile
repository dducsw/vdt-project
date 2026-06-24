PYTHON ?= python
PG_HOST ?= localhost
PG_PORT ?= 5434
PG_DB ?= thelook_db
PG_USER ?= db_user
PG_PASSWORD ?= db_password
PG_SCHEMA ?= demo
YEAR_SHIFT ?= 4

.PHONY: seed-snapshot full-load-gcs gendata gendata-once gendata-events resume-gendata reset-seed-gendata register-incremental-cdc

seed-snapshot:
	$(PYTHON) src/seed_from_csv.py --host $(PG_HOST) --port $(PG_PORT) --database $(PG_DB) --user $(PG_USER) --password $(PG_PASSWORD) --schema $(PG_SCHEMA) --data-dir ./data --truncate-first --year-shift $(YEAR_SHIFT) --skip-events

full-load-gcs:
	$(PYTHON) ../src/etl/local_pg_to_gcs.py --pg-host $(PG_HOST) --pg-port $(PG_PORT) --pg-database $(PG_DB) --pg-user $(PG_USER) --pg-password $(PG_PASSWORD) --pg-schema $(PG_SCHEMA)

gendata:
	PG_HOST=$(PG_HOST) PG_PORT=$(PG_PORT) PG_DB=$(PG_DB) PG_USER=$(PG_USER) PG_PASSWORD=$(PG_PASSWORD) PG_SCHEMA=$(PG_SCHEMA) $(PYTHON) run_generator.py

resume-gendata:
	PG_HOST=$(PG_HOST) PG_PORT=$(PG_PORT) PG_DB=$(PG_DB) PG_USER=$(PG_USER) PG_PASSWORD=$(PG_PASSWORD) PG_SCHEMA=$(PG_SCHEMA) AVG_QPS=5 $(PYTHON) run_generator.py

gendata-once:
	$(PYTHON) generator.py --db-host $(PG_HOST) --db-port $(PG_PORT) --db-user $(PG_USER) --db-password $(PG_PASSWORD) --db-name $(PG_DB) --db-schema $(PG_SCHEMA) --avg-qps 5 --init-num-users 0 --max-iter 100

gendata-events:
	$(PYTHON) generate_events_only.py --db-host $(PG_HOST) --db-port $(PG_PORT) --db-user $(PG_USER) --db-password $(PG_PASSWORD) --db-name $(PG_DB) --db-schema $(PG_SCHEMA) --avg-qps 5 --init-num-users 0 --max-iter -1

reset-seed-gendata:
	$(PYTHON) src/seed_from_csv.py --host $(PG_HOST) --port $(PG_PORT) --database $(PG_DB) --user $(PG_USER) --password $(PG_PASSWORD) --schema $(PG_SCHEMA) --data-dir ./data --truncate-first --year-shift $(YEAR_SHIFT) --skip-events
	PG_HOST=$(PG_HOST) PG_PORT=$(PG_PORT) PG_DB=$(PG_DB) PG_USER=$(PG_USER) PG_PASSWORD=$(PG_PASSWORD) PG_SCHEMA=$(PG_SCHEMA) $(PYTHON) run_generator.py

register-incremental-cdc:
	$(PYTHON) ../src/etl/debezium_incremental.py
