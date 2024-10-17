"""
Configuration handling.
"""
import json
import logging
import os
from datetime import timedelta
from typing import Optional

import boto3
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

SECRET_CACHE: Optional[SecretCache] = None

DEFAULT_SECRET_PATH = "/opus/opus-secrets"

MONIKERS = {"local": "OPUS-C-UW2",
            "test": "OPUS-C-UW2",
            "development": "OPUS-C-UW2",
            "staging": "OPUS-S-UW2",
            "stg-fr": "OPUS-SFR-UE2",
            "production": "OPUS-P-UW2",
            "prod-fr": "OPUS-PFR-UE2"}


class Config:
    """
    Config class.
    """
    def __init__(self, service, config):
        """
        Initialization function.

        Args:
            service: The name of the service in the configuration file.
            config: The stack environment ("development", "staging",
                    "production", etc.).
        """
        script_dir = os.path.dirname(__file__)
        file_name = config + ".json"
        config_file = os.path.join(script_dir, file_name)

        with open(config_file, "r", encoding="utf-8") as json_file:
            content = json.load(json_file)
            services = content['services']
            for svc in services:
                if svc['name'] == service:
                    self.config = svc
                    break

            self.general = content['general']

        self.load_parameters()

    def load_parameters(self):
        """
        Replace the parameters in the config file with the corresponding SSM
        parameters.
        """
        for key, value in self.config.items():
            if isinstance(value, dict):
                for subkey, subvalue in value.items():
                    if isinstance(subvalue, str) and subvalue.startswith(("param-store:", "env:")):
                        param_value = self.__get_parameter(subvalue)
                        self.config[key][subkey] = param_value
            if isinstance(value, str) and value.startswith(("param-store:", "env:")):
                param_value = self.__get_parameter(value)
                self.config[key] = param_value

    def __get_parameter(self, key):
        """
        Searches for parameter key first in the environment, then in secrets manager, then in parameter store
        """
        if key.startswith("env:"):
            param_value = os.getenv(key.replace("env:", ""))
        elif key.startswith("param-store:"):
            key = key.replace("param-store:", "")
            session = boto3.Session(region_name=self.general['region'])
            ssm = session.client('ssm')
            parameter = ssm.get_parameter(Name=key, WithDecryption=True)
            param_value = parameter['Parameter']['Value']
        return param_value

    def get_config(self):
        """
        Returns the config depending on the service and environment specified
        during initialization.

        Returns:
            A dictionary for the service configuration.
        """
        return self.config


class DynamoDBConfig:
    """
    DynamoDBConfig class.
    """

    def __init__(self, config):
        """
        Initialization function.

        Args:
            config: The stack environment ("development", "staging",
                    "production", etc.).
        """
        script_dir = os.path.dirname(__file__)
        file_name = config + ".json"
        tables_config = "tables.json"
        config_file = os.path.join(script_dir, "dynamodb", file_name)

        with open(config_file, "r", encoding="utf-8") as json_file:
            self.config_general = json.load(json_file)

        with open(os.path.join(script_dir, "dynamodb", tables_config), "r", encoding="utf-8") as tables_file:
            tables = json.load(tables_file)
            tables_s = json.dumps(tables).replace("{moniker}", MONIKERS.get(config))
            tables = json.loads(tables_s)
            self.config_tables = tables

    def get_config(self):
        """
        Returns the entire configuration.

        Returns:
            A dictionary for the configuration.
        """
        return {**self.config_general, **self.config_tables}

    def get_config_general(self) -> dict:
        """
        Returns the general configuration.

        Returns:
            A dictionary for the configuration.
        """
        return self.config_general['general']

    def get_config_tables(self) -> dict:
        """
        Returns the tables configuration.

        Returns:
            A dictionary for the configuration.
        """
        return self.config_tables['tables']

    def get_table_for_name(self, name: str):
        """
        Returns a table name if it ends with the passed name. This is to make moniker agnostic calls to dynamo.

        Returns:
            A string with the table name based on the configuration. If none found it returns the name passed.
        """

        tables_config = self.get_config_tables()
        for n in tables_config.keys():
            if n.endswith(name):
                return n
        return name

    def get_region(self):
        """
        convenience method to get the configured region
        :return: aws region for dynamodb and other services and assets
        """
        return self.get_config_general()['region']
