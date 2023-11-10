import boto3
from botocore.exceptions import ClientError
import json
import datetime


# An Environment Variable must be defined:
# AWS_PROFILE = <name of the profile you need to point to>
# The profile configurations are defined in the .aws/credentials file.

# EC2 resource object oriented API
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2/service-resource/index.html
ec2 = boto3.resource('ec2')
# Documentation for the instance-state-code field
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2/service-resource/instances.html
RUNNING_INSTANCES_STATE_CODE = 16

# For the RDS service only the low level API is available.
rds_client = boto3.client('rds')


# Subclass JSONEncoder for serializing datetime values
class DateTimeEncoder(json.JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


def get_ec2_vm_list():
    """
    Get the ID list of all running virtual machines in AWS EC2.
    The Object Oriented API for EC2 (resources) is available.
    :return: list of all the running virtual machines in EC2
    """
    result = list()
    for instance in ec2.instances.filter(
            Filters=[{'Name': 'instance-state-code', 'Values': [str(RUNNING_INSTANCES_STATE_CODE)]}], MaxResults=20):
        result.append(instance)
    return result


def get_rds_db_list():
    """
    Get the list of all the available DB instances in AWS RDS.
    The API is not Object Oriented, so the low level API with paginators is used.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds/client/describe_db_instances.html
    :return: list of the available databases in AWS RDS
    """
    result = list()
    paginator = rds_client.get_paginator('describe_db_instances').paginate()
    for page in paginator:
        for db_instance in page['DBInstances']:
            result.append(db_instance)
    return result


def turnoff_ec2_vms(ec2_vm_list, dry_run: bool):
    """
    Turn of all the running virtual machines of the list ec2_vm_list
    :param ec2_vm_list: The list of running VMs
    :param dry_run: Indicates if we are executing a dry run or not
    """
    for instance in ec2_vm_list:
        print(f'Stopping VM: {instance.id}')
        try:
            instance.stop(Hibernate=False, DryRun=dry_run)
        except ClientError as ex:
            print(f'Exception: {ex}')


def stop_rds_dbs(rds_db_list, dry_run: bool):
    """
    Stops all the available RDS databases.
    :param rds_db_list: the list of all databases that must be stopped.
    :param dry_run: Indicates if we are executing a dry run or not
    """
    for db_instance in rds_db_list:
        print(f'Stopping DB: {json.dumps(db_instance, cls=DateTimeEncoder)}')
        if dry_run:
            continue
        # this boto3 operation does not support dry runs.
        if db_instance['DBInstanceStatus'] == 'stopped':
            print(f"DB Instance {db_instance['DBInstanceIdentifier']} is stopped")
        else:
            rds_client.stop_db_instance(DBInstanceIdentifier=db_instance['DBInstanceIdentifier'])


def lambda_handler(event, context):
    print('getting vm list')
    vm_list = get_ec2_vm_list()
    print('got ec2 list')
    turnoff_ec2_vms(vm_list, False)

    db_list = get_rds_db_list()
    stop_rds_dbs(db_list, False)
