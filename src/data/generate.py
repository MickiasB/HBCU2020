from collections import defaultdict
import os
import dpath
from dynamodb.dynamodb import DynamoDB, DynamoDBConfig
from s3.s3 import S3Config, S3Uploader
from config.config import Config
from data.epss import EPSS


class DataLoaderS3Config(S3Config):
    """
    DataLoaderS3Config class.
    """
    def __init__(self, env: str, bucket_name: str, url: str, access_key: str, secret_key: str, region_name: str):
        S3Config.__init__(self, env)
        self._bucket = bucket_name
        self._url = url
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region_name

    @property
    def bucket(self):
        """
        Get raw data bucket.
        """
        return self._bucket
    
    @property
    def locals3_url(self):
        """
        Get local minio s3 url.
        """
        return self._url
    
    @property
    def mino_access_key(self):
        """
        Get minio access key.
        """
        return self._access_key
    
    @property
    def minio_secret_key(self):
        """
        Get minio secret key.
        """
        return self._secret_key
    
    @property
    def region(self):
        """
        Get region.
        """
        return self._region


def store_in_s3(s3_uploader, vuln_data):
    """
    Store vulnerability data in S3
    """
    source = "EPSS"
    qualifier = "findings"
    filename = "vulnerability_data"
    s3_uploader.store_modeled_data(source, qualifier, filename, vuln_data)


def store_in_dynamodb(vuln_data, metadata_table):
    """
    Store vulnerability data in DynamoDB
    """
    metadata_table.big_batch_put(vuln_data, batch_size=1000, max_workers=10)
    

def insert_vulnerability_data():
    """
    Load vulnerabilities from EPSS
    """
    config = Config("EPSS", os.environ.get("CONFIG", "test")).get_config()
    dynamo_config = DynamoDBConfig(os.environ.get("CONFIG", "test"))
    metadata_table = DynamoDB(dynamo_config.get_config(), "SampleTable")
    s3_env = str(dpath.get(config, "/s3/env", default="default"))
    bucket = str(dpath.get(config, "/s3/Bucket", default="default"))
    locals3_url = str(dpath.get(config, "/s3/locals3_url", default="default"))
    minio_access_key = str(dpath.get(config, "/s3/minio_access_key", default="default"))
    minio_secret_key = str(dpath.get(config, "/s3/minio_secret_key", default="default"))
    region = str(dpath.get(config, "/s3/region", default="default"))
    s3_config = DataLoaderS3Config(s3_env, bucket, locals3_url, minio_access_key, minio_secret_key, region)
    s3_uploader = S3Uploader(s3_config)


    epss = EPSS(config)
    vuln_data = epss.collect_vulnerabilities()
    cve_epss = []
    cve_details = []
    for entry in vuln_data:
        epss_details = {k: v for k, v in entry['epss_details'].items() if k != 'cve_id'}
        cve_epss.append({
            "hashKey": entry.get('epss_details').get('cve_id'),
            "sortKey": "cve#epss",
            **epss_details
        })
        product_item = {
            k:v for k, v in entry.items() if k != 'epss_details' and k != "cve_id"
        }
        cve_details.append({
            "hashKey": entry.get('cve_id'),
            "sortKey": "cve#details",
            **product_item
        })
    product_cve_map = defaultdict(list)  # Initialize a defaultdict with lists
    
    # Populate the product_cve_map
    [product_cve_map[product].append(entry["cve_id"]) for entry in vuln_data for product in entry["productList"]]
    
    # Convert to the desired structure
    result = [
        {"hashKey": product, "sortKey": "product#cve", "cve_list": cve_list}
        for product, cve_list in product_cve_map.items()
    ]
    result = [*result, *cve_epss, *cve_details]
    
     
    
    print(f"Storing {len(result)} records in S3")
    store_in_s3(s3_uploader, result)
    
    print(f"Inserting {len(result)} records into DynamoDB")
    store_in_dynamodb(result, metadata_table)
    

    
