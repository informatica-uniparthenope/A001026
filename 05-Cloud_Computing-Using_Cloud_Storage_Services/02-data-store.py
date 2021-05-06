import boto3
import csv

s3 = boto3.resource('s3')
dyndb=boto3.resource('dynamodb',region_name='us-west-2')

base_path="/Users/raffaelemontella/cloud_computing/storage/aws"
bucket_name="raffmont-datacont-2020-2021"

try:
	s3.meta.client.head_bucket(Bucket=bucket_name)
except ClientError:
	s3.create_bucket(
        	Bucket = bucket_name,
        	CreateBucketConfiguration={
                	'LocationConstraint': 'us-west-2'
        	}
	)

table = None
try:
	table = dyndb.create_table(TableName='DataTable',
		KeySchema =[
			{'AttributeName':'PartitionKey','KeyType': 'HASH'},
			{'AttributeName':'RowKey','KeyType':'RANGE'}
		],
		AttributeDefinitions=[
			{'AttributeName':'PartitionKey','AttributeType':'S'},
			{'AttributeName':'RowKey','AttributeType': 'S' }
		],
		BillingMode="PAY_PER_REQUEST"
	)
	table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
except:
	table = dyndb.Table("DataTable")

urlbase = "https://s3-us-west-2.amazonaws.com/"+bucket_name+"/"
with open( base_path+"/experiments.csv", 'r') as csvfile:
	csvf = csv.reader(csvfile,
		delimiter =',',
		quotechar='|'
	)

	for item in csvf:
		body=open(base_path+'/datafiles/'+item[3],'rb')
		s3.Object(bucket_name,item[3]).put(Body=body)
		md=s3.Object(bucket_name,item[3]).Acl().put(ACL='public-read')
		url= urlbase +item [3]
		metadata_item={
			'PartitionKey':item[0],
			'RowKey':item[1],
			'Description':item[4],
			'Date':item[2],
			'Url':url
		}
		table.put_item(Item= metadata_item)
