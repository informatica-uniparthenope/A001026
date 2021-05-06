# Code to stop/run an EC2 instance (configured with 'aws configure')

import boto3
from botocore.exceptions import ClientError

EC2_TYPE = 'ON'

# Function used to get the first EC2 instance ID
def getFirstInstanceID(ec2):
    # Get all EC2 instances
    all_instances = ec2.describe_instances()
    # Select the first
    first = all_instances['Reservations'][0]['Instances'][0]
    # Return the ID
    return first['InstanceId']

def main():
    ec2 = boto3.client('ec2')
    if EC2_TYPE == 'ON':
        # Do a dryrun first to verify permissions
        try:
            ec2.start_instances(InstanceIds=[getFirstInstanceID(ec2)], DryRun=True)
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise

        # Dry run succeeded, run start_instances without dryrun
        try:
            response = ec2.start_instances(InstanceIds=[getFirstInstanceID(ec2)], DryRun=False)
            print(response)
        except ClientError as e:
            print(e)
    else:
        # Do a dryrun first to verify permissions
        try:
            ec2.stop_instances(InstanceIds=[getFirstInstanceID(ec2)], DryRun=True)
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise

        # Dry run succeeded, call stop_instances without dryrun
        try:
            response = ec2.stop_instances(InstanceIds=[getFirstInstanceID(ec2)], DryRun=False)
            print(response)
        except ClientError as e:
            print(e)

if __name__ == "__main__":
    main()