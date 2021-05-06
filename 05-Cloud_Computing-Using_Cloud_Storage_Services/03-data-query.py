import boto3
from boto3.dynamodb.conditions import Key
import csv

# https://www.fernandomc.com/posts/ten-examples-of-getting-data-from-dynamodb-with-python-and-boto3/

s3 = boto3.resource('s3')
dyndb=boto3.resource('dynamodb',region_name='us-west-2')

bucket_name="raffmont-datacont-2020-2021"

table = dyndb.Table("DataTable")

response = table.query(
	KeyConditionExpression=Key('PartitionKey').eq('experiment1')
)


for item in response["Items"]:
	print(item)
