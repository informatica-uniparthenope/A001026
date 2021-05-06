import boto3
from botocore.exceptions import ClientError

def getFirstInstanceID(ec2):
    # Get all EC2 instances
    all_instances = ec2.describe_instances()
    # Select the first
    first = all_instances['Reservations'][0]['Instances'][0]
    # Return the ID
    return first['InstanceId']

def main():
    # Create an EC2 Client
    ec2 = boto3.client('ec2')
    try:
        ec2.reboot_instances(InstanceIds=[getFirstInstanceID(ec2)], DryRun=True)
    except ClientError as e:
        if 'DryRunOperation' not in str(e):
            print("You don't have permission to reboot instances.")
            raise

    try:
        response = ec2.reboot_instances(InstanceIds=[getFirstInstanceID(ec2)], DryRun=False)
        print('Success', response)
    except ClientError as e:
        print('Error', e)


if __name__ == "__main__":
    main()