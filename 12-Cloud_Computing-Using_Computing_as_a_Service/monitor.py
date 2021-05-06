import boto3

EC2_TYPE = 'UNMONITOR'


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

    if EC2_TYPE is 'MONITOR':
        response = ec2.monitor_instances(InstanceIds=[getFirstInstanceID(ec2)])
    elif EC2_TYPE is 'UNMONITOR':
        response = ec2.unmonitor_instances(InstanceIds=[getFirstInstanceID(ec2)])


if __name__ == "__main__":
    main()


