import boto3
import sys
import argparse

from botocore.exceptions import ClientError

ap = argparse.ArgumentParser()
ap.add_argument("-m","--Action", required=True, type=int, default=True,
        help="Action: 0 to Stop -- 1 to Start")
ap.add_argument("-i","--ID_Instance", required=True, type=str,
        help="The ID of the instance to monitor")
args = vars(ap.parse_args())

# Input Parameters
Action = int(args["Action"])
ID_Instance = args["ID_Instance"]

# Creation of the instance EC2
ec2 = boto3.client('ec2')

# If the action is to start EC2 instance
if Action == 1:

    # The option Dryrun = True checks whether we have the permission for making this
    # action, where in this case is to start a EC2 instance specified with its ID
    try:
            ec2.start_instance(Instance_Ids=[ID_Instance], DryRun=True)

    except ClientError as e:
        if 'DryRunOperation' not in str(e):
            raise

    # If we have the permission to do this action, so we can start the EC2 instance, without
    # the option DryRun
    try:
        response = ec2.start_instance(Instance_Ids=[ID_Instance], DryRun=False)
        print(response)
    except ClientError as e:
        print(e)

# Otherwise, the action is to stop the EC2 instance
else:
    # First of all, we have always to verify if we have the permission to do that action
    try:
        ec2.stop_instance(Instance_Ids=[ID_Instance], DryRun=True)

    except ClientError as e:
        if 'DryRunOperation' not in str(e):
            raise

    # If we have the permission to do this action, so we can stop the EC2 instance, without
    # the option DryRun
    try:
        response = ec2.stop_instance(Instance_Ids=[ID_Instance], DryRun=False)
        print(response)
    except ClientError as e:
        print(e)
