# Simple code to describe all EC2 instances from a user (configured with 'aws configure')
import boto3

def main():
    # Create an EC2 Client
    ec2 = boto3.client('ec2')

    # Get all EC2 instances
    all_instances = ec2.describe_instances()

    # Show EC2 instances
    print(all_instances)

if __name__ == "__main__":
    main()