import boto3
import sys
import argparse

from botocore.exceptions import ClientError

ap = argparse.ArgumentParser()
ap.add_argument("-i","--ID_Instance", required=True, type=str,
        help="The ID of the instance to monitor")
args = vars(ap.parse_args())

# Input Parameters
ID_Instance = args["ID_Instance"]

# Creation of the instance EC2
ec2 = boto3.client('ec2')

# Before to reboot the EC2 instance identified by ID, we have to verify whether we have the
# permission to do the reboot it.
# We can check our permission on the instance, setting a true the DryRun option of the method
# reboot_instances
try:
    ec2.reboot_instance(Instance_Ids=[ID_Instance], DryRun=True)

except ClientError as e:
    if 'DryRunOperation' not in str(e):
        raise

# If we have the permission to do this action, so we can reboot the EC2 instance, without
# the option DryRun
try:
    response = ec2.reboot_instance(Instance_Ids=[ID_Instance], DryRun=False)
    print(response)
except ClientError as e:
    print(e)
