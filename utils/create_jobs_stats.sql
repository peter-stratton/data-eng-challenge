DROP TABLE IF EXISTS job_stats;
CREATE TABLE job_stats (
id varchar(50),
app_version varchar(50),
execution_date date,
execution_ts timestamp,
query_start_date date,
query_stop_date date,
job_successful bool,
job_exception text
)
