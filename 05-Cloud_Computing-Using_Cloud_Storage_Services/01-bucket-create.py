import boto3
s3 = boto3.resource('s3')
s3.create_bucket(
	Bucket = 'raffmont-datacont-2020-2021',
	CreateBucketConfiguration={
		'LocationConstraint': 'us-west-2'
	}
)

