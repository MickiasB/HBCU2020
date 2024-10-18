from concurrent.futures import ThreadPoolExecutor
import time
import boto3
import os
import json
from dotenv import load_dotenv


URL="http://localhost:8000"


def _retry_batch_writer_put_item(writer, item, tablename, item_idx, total_items):
        """
        Private Helper function to retry batch writer, so that Opus can retry writing to the DynamoDB table
        if DynamoDB throttles the call because of some client error like resizing.

        Args:
            writer: Batch writer.
            item: Item to be written.
            tablename: Name of the DynamoDB table. (Unused argument, but useful for capturing state of the arguments).
            item_idx: Index of the item in the current batch. (Unused argument,
            but useful for capturing state of the arguments).
            total_items: Total items in the batch. (Unused argument, but useful for capturing state of the arguments).

        Returns:
            Whatever put_item returns.
        """
        del tablename
        del item_idx
        del total_items
        return writer.put_item(Item=item)


class DynamoDB:
    def __init__(self, config, table_name, full=True):
        self.config = config
        self.table_name = table_name
        session = boto3.Session(region_name=config['general']['region'])
        self.resource = session.resource('dynamodb', endpoint_url=self.config['general']['endpointURL'])
        if self.resource is None:
            print("Error connecting to DynamoDB resource!")
        self.client = session.client('dynamodb', endpoint_url=self.config['general']['endpointURL'])
        if self.client is None:
            print("Error connecting to DynamoDB client!")

        self.table = self.initialize(full)
        if self.table is None:
            print("Error initializing the table!")


    def initialize(self, full):
        """
        Initialize the table.

        Args:
            full: A boolean for full db table configuration.

        Returns:
            The boto3 DynamoDB object or None.
        """
        existing_tables = self.client.list_tables()['TableNames']
        if self.table_name in existing_tables:
            self.configure(full)
            return self.resource.Table(self.table_name)

        self.create_table()

        if self.is_table_active():
            self.configure(full)
            return self.resource.Table(self.table_name)

        return None


    def configure(self, full):
        """
        Configure/update table properties if necessary.

        Args:
            full: A boolean for full db table configuration.
        """
        if full:
            self.update_global_indices()


    def is_table_active(self):
        """
        Checks if the table is active or if it's still being created.

        Returns:
            A boolean indicating table active status.
        """
        while True:
            time.sleep(5)
            if self.client.describe_table(TableName=self.table_name)['Table']['TableStatus'] == "ACTIVE":
                break
        return True


    def update_global_indices(self):
        """
        Update the global secondary indexes on a DynamoDB table. This only adds a new GSI if its not present.

        Returns:
            None
        """
        configured_indices = {
            gsi['IndexName']: gsi for gsi in self.config['tables'][self.table_name].get('globalSecondaryIndexes', [])
        }
        configured_attributes = {
            attr['AttributeName']: attr for attr in self.config['tables'][self.table_name]['attributeDefinitions']
        }
        response = self.client.describe_table(TableName=self.table_name)
        # check response error
        table_indices = {gsi["IndexName"]: gsi for gsi in response["Table"].get("GlobalSecondaryIndexes", [])}
        table_attributes = {attribute['AttributeName']: attribute for attribute in response['Table'].get(
            'AttributeDefinitions', [])}
        indices_to_create = []
        for index_name, gsi in configured_indices.items():
            if index_name not in table_indices:
                indices_to_create.append(gsi)

        attr_to_create = []
        for attr_name, attr in configured_attributes.items():
            if attr_name not in table_attributes:
                attr_to_create.append(attr)
        update_table_args = {
            'TableName': self.table_name
        }
        update_table = False
        if indices_to_create:
            update_table_args['GlobalSecondaryIndexUpdates'] = [
                {"Create": index} for index in indices_to_create
            ]
            update_table_args['AttributeDefinitions'] = list(configured_attributes.values())
            update_table = True
        elif attr_to_create:
            update_table_args['AttributeDefinitions'] = attr_to_create
            update_table = True

        if update_table:
            response = self.client.update_table(**update_table_args)

    def create_table(self):
        """
        Create an on-demand table without provisioning.
        """
        args = {"TableName": self.table_name,
                "KeySchema": self.config['tables'][self.table_name]['keySchema'],
                "ProvisionedThroughput": self.config['general']['provisionedThroughput'],
                "AttributeDefinitions": self.config['tables'][self.table_name]['attributeDefinitions'],
                "SSESpecification": self.config['general']['sseSpecification'],
                "Tags": self.config['general']['tags']}

        if 'globalSecondaryIndexes' in self.config['tables'][self.table_name]:
            args.update({"GlobalSecondaryIndexes": self.config['tables'][self.table_name]['globalSecondaryIndexes']})
        self.resource.create_table(**args)

    def put_item(self, item):
        """
        Put an item into the database.

        Args:
            item: The item to insert into the database.
        """
        self.table.put_item(Item=item)

    def big_batch_put(self, items: list, batch_size: int, max_workers: int = 20):
        """
        Experimental, multithreaded batch put
        Args:
            items: all items going to the table
            batch_size: size of each batch submitted to the batch_put function
            max_workers: maximum threads

        Returns:

        """
        batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(self.batch_put, batches)

    def batch_put(self, items: list):
        """
        Uses the batch writer to write items to table
        Args:
            items: List of items to insert into table

        Returns:

        """
        with self.table.batch_writer() as writer:
            for i, item in enumerate(items, 1):
                _retry_batch_writer_put_item(writer, item, self.table_name, i, len(items))
        print(f"Batch loaded data into table {self.table_name}")
        
    def query(self,
              condition,
              filter_expression=None,
              index_name=None) -> list:

        results = []

        start_key = None
        while True:
            response, start_key = self.batch(condition, filter_expression,
                                             index_name, start_key)
            results.extend(response)
            if not start_key:
                break

        return results
    
    def batch(self, condition, filter_expression=None, index_name = None, eval_key = None):
        query_fields = {
            'TableName': self.table_name,
            'KeyConditionExpression': condition
        }
        if filter_expression:
            query_fields.update({"FilterExpression": filter_expression})
        if index_name:
            query_fields.update({"IndexName": index_name})
        if eval_key:
            query_fields.update({'ExclusiveStartKey': eval_key})
        response = self.table.query(**query_fields)
        results = response.get('Items', [])
        eval_key = response.get('LastEvaluatedKey', None)
        return results, eval_key
    
    def scan(self):
        """
        Get all items from the table.

        Returns:
            A list of the items from the table.
        """
        results = []
        query_fields = {
            'TableName': self.table_name,
        }
        done = False
        start_key = None
        while not done:
            if start_key:
                query_fields.update({'ExclusiveStartKey': start_key})
            response = self.table.scan(**query_fields)
            results.extend(response.get('Items', []))
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
        return results
        


class DynamoDBConfig:
    """
    DynamoDBConfig class.
    """

    def __init__(self, config):
        """
        Initialization function.
        """
        script_dir = os.path.dirname(__file__)
        file_name = config + ".json"
        tables_config = "tables.json"
        config_file = os.path.join(script_dir, file_name)
        with open(config_file, "r", encoding="utf-8") as json_file:
            self.config_general = json.load(json_file)
        with open(os.path.join(script_dir, tables_config), "r", encoding="utf-8") as tables_file:
            tables = json.load(tables_file)
            self.config_tables = tables


    def get_config(self):
        """
        Returns the entire configuration.

        Returns:
            A dictionary for the configuration.
        """
        return {**self.config_general, **self.config_tables}


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
        self.config = {}

        with open(config_file, "r", encoding="utf-8") as json_file:
            content = json.load(json_file)
            services = content['services']
            for svc in services:
                if svc['name'] == service:
                    self.config = svc
                    break

    def get_config(self):
        """
        Returns the config depending on the service and environment specified
        during initialization.

        Returns:
            A dictionary for the service configuration.
        """
        return self.config