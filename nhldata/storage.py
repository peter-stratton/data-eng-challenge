from dataclasses import dataclass


@dataclass
class StorageKey:
    game_year: str
    game_month: str
    game_day: str
    game_id: str

    def key(self):
        """ renders the s3 key for the given set of properties """
        return '/'.join([self.game_year, self.game_month, self.game_day, f'{self.game_id}.csv'])


class Storage:
    def __init__(self, data_bucket, jobs_bucket, s3_client):
        self._s3_client = s3_client
        self.data_bucket = data_bucket
        self.jobs_bucket = jobs_bucket

    def store_game(self, key: StorageKey, game_data: str) -> bool:
        self._s3_client.put_object(Bucket=self.data_bucket, Key=key.key(), Body=game_data)
        return True

    def store_job(self, key: str, job_data: str) -> bool:
        self._s3_client.put_object(Bucket=self.jobs_bucket, Key=key, Body=job_data)
        return True
