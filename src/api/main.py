import os
import pathlib
from ariadne.asgi import GraphQL
from ariadne import QueryType, make_executable_schema, load_schema_from_path
from boto3.dynamodb.conditions import Attr, Key
from dynamodb.dynamodb import DynamoDB, DynamoDBConfig
# Initialize query
query = QueryType()

# Define resolvers


@query.field("listProducts")
def listProducts(_, info, productName=None):
    config = DynamoDBConfig(os.environ.get("CONFIG", "test"))
    cve_table = DynamoDB(config.get_config(), "SampleTable")
    results = []
    if productName:
        condition = Key("hashKey").eq(productName) & Key("sortKey").eq("product#cve")
        response = cve_table.query(condition)
        results = [
            {"name": item["hashKey"], "CVEList": item["cve_list"]}
            for item in response
        ]
        

    else:
        condition = Key("sortKey").eq("product#cve")
        response = cve_table.query(condition, index_name="AssessmentBySourceIndex")
        results = [
            {"name": item["hashKey"], "CVEList": item["cve_list"]}
            for item in response
        ]
    
    return results
        

@query.field("listCVEDetails")
def listCVEDetails(_, info, cve=None):
    config = DynamoDBConfig(os.environ.get("CONFIG", "test"))
    cve_table = DynamoDB(config.get_config(), "SampleTable")
    results = []
    if cve:
        condition = Key("hashKey").eq(cve) & Key("sortKey").eq("cve#details")
        response = cve_table.query(condition)
        results = [
            {"cve": item["hashKey"], **item}
            for item in response
        ]
        

    else:
        condition = Key("sortKey").eq("cve#details")
        response = cve_table.query(condition, index_name="AssessmentBySourceIndex")
        results = [
            {"cve": item["hashKey"], **item}
            for item in response
        ]
    
    return results  

@query.field("listEPSS")
def listEPSS(_, info, cve=None):
    config = DynamoDBConfig(os.environ.get("CONFIG", "test"))
    cve_table = DynamoDB(config.get_config(), "SampleTable")
    results = []
    if cve:
        condition = Key("hashKey").eq(cve) & Key("sortKey").eq("epss#cve")
        response = cve_table.query(condition)
        results = [
            {"cve": item["hashKey"], **item}
            for item in response
        ]
        

    else:
        condition = Key("sortKey").eq("cve#epss")
        response = cve_table.query(condition, index_name="AssessmentBySourceIndex")
        results = [
            {"cve": item["hashKey"], **item}
            for item in response
        ]
    
    return results     
        
    
   

# Create executable schema
type_defs = load_schema_from_path(f"{pathlib.Path(__file__).parent.resolve()}/schemas")
schema = make_executable_schema(type_defs, query)
app = GraphQL(schema)