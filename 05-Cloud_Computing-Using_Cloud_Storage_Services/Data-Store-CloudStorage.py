import boto3
import csv

# Import Driver for Amazon AWS
from cloudstorage.drivers.amazon import S3Driver

# Name of the Backet
bucket_name=""

# For more details of the installation, follow this link: https://pypi.org/project/cloudstorage/
# For more details of the documentation, follow this link: https://cloudstorage.readthedocs.io/en/latest/api/container.html#cloudstorage.base.Container.upload_blob

# Creation of the reference for Amazon S3
s3 = S3Driver(key='<my-aws-access-key-id>', secret='<my-aws-secret-access-key>')

# With the function "creare_container", we create a new container in Amazon S3 with the
# name specified in the string bucket_name
# In the case of existence of a container with backet_name, the function return the instance of if.
container = storage.create_container(bucket_name)

dyndb=boto3.resource('dynamodb',region_name='us-west-2')

# Path of the directory where are contined the our credentials of AWS.
base_path="<path_of_the_credentials"

# Creation of a table in DynamoDB where we going to push the metadata of CSV File
table = None
try:

	# In the case that the table not exist DynamoDB yet
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

	# The function "get_waiter" awaits until the creation of the table specified before
	# is terminated
	table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
except:
	# If the table exists, we recover the its pointer
	table = dyndb.Table("DataTable")


urlbase = "https://s3-us-west-2.amazonaws.com/"+bucket_name+"/"

# We try to open the CSV file and we split all fields contained in it
with open( base_path+"/experiments.csv", 'r') as csvfile:
	csvf = csv.reader(csvfile,
		delimiter =',',
		quotechar='|'
	)

	for item in csvf:
		body=open(base_path+'/datafiles/'+item[3],'rb')

		# Memorize the BLOB Object in the container of Amazon S3 and upload it
		s3.Object(bucket_name,item[3]).put(Body=body)
		blob = s3.upload_blob(filename=body,blob_name=item[3],acl='public-read')

		# We create an entry in the table of DynamoDB
		url= urlbase +item [3]
		metadata_item={
			'PartitionKey':item[0],
			'RowKey':item[1],
			'Description':item[4],
			'Date':item[2],
			'Url':url
		}
		table.put_item(Item=metadata_item)
