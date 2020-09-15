from dataclasses import dataclass


@dataclass
class JobMetadata:
    """Class for keeping track job run metadata"""
    id: str
    app_version: str
    execution_date: str
    execution_ts: str
    query_start_date: str
    query_stop_date: str
    job_successful: str
    job_exception: str
