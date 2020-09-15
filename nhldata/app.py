import logging
import os
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta

import boto3
import click
from botocore.config import Config

from nhldata import __version__
from nhldata.metadata import JobMetadata
from nhldata.nhl import AdapterFactory
from nhldata.storage import Storage

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
API_FACTORY = AdapterFactory()
DATE_FORMATS = ['%Y-%m-%d']


def splash(debug: bool):
    click.echo('\nNHLData v%s' % __version__)
    click.echo('NHLData log level is %s and higher\n' % ('DEBUG' if debug else 'INFO'))


@click.group()
@click.option('--debug/--no-debug', default=False)
def main(debug):
    if debug:
        logging.basicConfig(format=LOG_FORMAT, level=logging.DEBUG)
    else:
        logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    splash(debug)


@main.command()
@click.option('--api-version', type=click.Choice(API_FACTORY.all_versions(), case_sensitive=False), default='v1',
              help="NHL statsapi version to target", show_default=True)
@click.option('--from-date', type=click.DateTime(formats=DATE_FORMATS),
              default=(datetime.now() - timedelta(days=1)).strftime(DATE_FORMATS[0]),
              help="Start date of retrieval window", show_default=True)
@click.option('--to-date', type=click.DateTime(formats=DATE_FORMATS),
              default=datetime.now().strftime((DATE_FORMATS[0])),
              help="End date of retrieval window", show_default=True)
def games(api_version, from_date, to_date):
    meta = JobMetadata(
        id=str(uuid.uuid4()),
        app_version=__version__,
        execution_date=datetime.utcnow().strftime("%Y/%m/%d"),
        execution_ts=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        query_start_date=from_date.strftime(DATE_FORMATS[0]),
        query_stop_date=to_date.strftime(DATE_FORMATS[0]),
        job_successful='True',
        job_exception=''
    )
    bucket = os.environ.get('DEST_BUCKET', 'output')
    jobs = os.environ.get('JOB_BUCKET', 'jobs')
    s3client = boto3.client('s3', config=Config(signature_version='s3v4'),
                            endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
    storage = Storage(bucket, jobs, s3client)
    try:
        api_adapters = API_FACTORY.adapter_for_version(api_version)
        api = api_adapters.api()
        crawler = api_adapters.crawler(api, storage)
        crawler.crawl(from_date, to_date)
    except Exception as e:
        click.echo('JOB RUN FAILED')
        click.echo(e)
        # if it blows up, update the meta object
        meta.job_successful = 'False'
        meta.job_exception = e.__repr__().replace(',', ' ')
    finally:
        meta_keys = ','.join(asdict(meta).keys())
        meta_values = ','.join(asdict(meta).values())
        storage_key = f'{meta.execution_date}/{meta.id}.csv'
        csv_string = f'{meta_keys}\n{meta_values}'
        storage.store_job(storage_key, csv_string)
