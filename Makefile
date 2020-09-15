
.PHONY: init sort test lint coverage step1 step2 catalog_data run_sql dbt_run dbt_test


init:
	@pip install -r requirements/dev.txt
	@pip install -e .

sort:
	@isort .

test: sort
	@pytest -vv

lint: sort
	@flake8 .

coverage: test
	@open ./htmlcov/index.html

clean:
	@rm -rf s3_data && mkdir s3_data

mkbucket:
	@mkdir -p s3_data/{data,job}-bucket && touch s3_data/data-bucket/.gitkeep

down:
	@docker-compose down

step1: down clean mkbucket
	@docker-compose up --build
	
#--- part two
catalog_data:
	@[ -f s3_data/load_data.sql ] && rm s3_data/load_data.sql; \
	  find s3_data/data-bucket -name "*.csv" -exec echo "\copy game_stats from '{}' WITH (FORMAT csv, HEADER);" \; >> s3_data/load_data.sql && \
	  find s3_data/job-bucket -name "*.csv" -exec echo "\copy job_stats from '{}' WITH (FORMAT csv, HEADER);" \; >> s3_data/load_data.sql \

run_sql: catalog_data
	@docker exec \
	  -w /data \
	  $$(basename $(PWD))_db_1 \
	  psql -h db -U postgres -f s3_data/load_data.sql

dbt_run:
	@docker run --rm \
	  --network host \
	  -v $(PWD)/dbt:/usr/app \
	  -v $(PWD)/.dbt:/root/.dbt \
	  fishtownanalytics/dbt:0.17.2 run

dbt_test:
	@docker run --rm \
	  --network host \
	  -v $(PWD)/dbt:/usr/app \
	  -v $(PWD)/.dbt:/root/.dbt \
	  fishtownanalytics/dbt:0.17.2 test

step2: run_sql dbt_run dbt_test

points_leaders:
	@docker run --rm  \
	  --network host \
	  -e PGPASSWORD=password \
	  --entrypoint psql \
	  postgres:12-alpine \
	  -h localhost -U postgres -c "select * from points_leaders order by points desc limit 10;"

job_stats:
	@docker run --rm  \
	  --network host \
	  -e PGPASSWORD=password \
	  --entrypoint psql \
	  postgres:12-alpine \
	  -h localhost -U postgres -c "select * from job_stats order by execution_ts desc limit 10;"
