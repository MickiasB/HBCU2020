""" Provides methods for storing Raw data in AWS S3 """

import gzip
import io
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from minio import Minio
from minio.error import S3Error
import awswrangler as wr
import boto3
import pandas as pd
import pytz
from botocore.exceptions import ClientError
from pandas import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq

from .s3_config import S3Config

DESTINATION_PATH_FORMAT = "{source}/{qualifier}/{date}/{filename}-{epoch}"
S3_PATH_REGEX = re.compile(
    r"s3://(?P<bucket>[\w| -]*)/"  # bucket
    r"(?P<source>\w*)/"  # source
    r"(?P<qualifier>\w*)/"  # qualifier
    r"(?P<date>\d{4}/\d{2}/\d{2})/"  # date
    r"(?P<filename>\w*-\d{10}.(?:gzip|parquet))"  # filename
)


@dataclass(frozen=True)
class TimeStamp:
    """ Gets time string formats """
    date: str
    epoch: str


def get_timestamp() -> TimeStamp:
    """ Return time in string format """
    current_time = datetime.now(pytz.utc)
    return TimeStamp(
        date=current_time.strftime("%Y/%m/%d"),
        epoch=current_time.strftime('%s')
    )


def generate_s3_path(source: str, qualifier: str, filename: str):
    """ Generates S3 path according to expected format (see DESTINATION_PATH_FORMAT)"""
    time_stamp = get_timestamp()
    return DESTINATION_PATH_FORMAT.format(
        source=source,
        qualifier=qualifier,
        date=time_stamp.date,
        filename=filename,
        epoch=time_stamp.epoch)


def findings_to_dataframe(findings: list) -> DataFrame:
    """
    Convenience function to create a pandas dataframe from a list of Findings objects
    """
    return pd.DataFrame(findings)


class ENVIRONMENT(Enum):
    """ Switch to facilitate testing with localstack """
    LOCALSTACK = "local"
    AWS = "aws"


class S3Uploader:  # pylint: disable=too-few-public-methods
    """ Class for processing Trust raw data """

    def __init__(self, config: S3Config):
        # Use default config from config.yaml if no configuratin class is specified.
        self.config = config
        self.env = self.config.env
        print(f"Environment: {self.env}")
        if self.env == ENVIRONMENT.LOCALSTACK.value:
            self.s3_client = self._get_minio_client()
        else:
            self.s3_client = self._get_s3_client()

    def _get_minio_client(self):
        minio_client = Minio(
            self.config.locals3_url,  # or the endpoint where your MinIO server is running
            access_key=self.config.mino_access_key,
            secret_key=self.config.minio_secret_key,
            secure=False  # set to True if using HTTPS
        )
        return minio_client
    def _get_s3_client(self):  # pylint: disable=no-self-use
        """ Gets location of Trust S3 Raw data bucket """
        session = boto3.session.Session()
        s3_client = session.client('s3')

        return s3_client


    def store_modeled_data(self, source: str, qualifier: str, filename: str, findings: list):
        """ Store modeled data as parquet in s3 """
        destination = f"{generate_s3_path(source, qualifier, filename)}.parquet"
        if self.env == ENVIRONMENT.LOCALSTACK.value:
            try:
                if not self.s3_client.bucket_exists(self.config.bucket):
                    self.s3_client.make_bucket(self.config.bucket)
                    print(f"Bucket {self.config.bucket} created.")
                self._store_locally(findings, destination)
            except S3Error as exc:
                print(f"Error occurred: {exc}")
        else:
            destination = f"{generate_s3_path(source, qualifier, filename)}.parquet"

            wr.s3.to_parquet(
                df=findings_to_dataframe(findings),
                path=f"s3://{self.config.bucket}/{destination}",
            )
    
    def _store_locally(self, findings: list, destination: str):
        """ Store data locally """
        df = pd.DataFrame(findings)
        table = pa.Table.from_pandas(df)
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer) 
        parquet_buffer.seek(0)
        self.s3_client.put_object(
            self.config.bucket,
            destination, 
            parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
