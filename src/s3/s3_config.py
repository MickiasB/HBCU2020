"""
Configuration class for the S3 Buckets.
"""
import pathlib
from abc import ABC

import yaml
from yaml.loader import SafeLoader

CONFIG_FILE = f"{pathlib.Path(__file__).parent.resolve()}/config.yaml"

class S3Config(ABC):  # pylint: disable=too-few-public-methods
    """ Configuration class for S3 """

    def __init__(self, _env: str, config_file: str = CONFIG_FILE):
        with open(config_file, encoding="utf-8") as file:
            self.config_data = yaml.load(file, Loader=SafeLoader)
        self._env = _env
        self.s3_data = self.config_data.get("s3", {}).get(_env, {})

    @property
    def env(self):
        """ Provides environment """
        return self._env
    @property
    def bucket(self):
        """ Provides raw data bucket name """
        return self.s3_data.get("Bucket")
    
    @property
    def locals3_url(self):
        """ Provides local minio s3 url """
        return self.s3_data.get("locals3_url")
    
    @property
    def mino_access_key(self):
        """ Provides minio access key """
        return self.s3_data.get("minio_access_key")

    @property
    def minio_secret_key(self):
        """ Provides minio secret key """
        return self.s3_data.get("minio_secret_key")
    
    @property
    def region(self):
        """ Provides region """
        return self.s3_data.get("region")
    

