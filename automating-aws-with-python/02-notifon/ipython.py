import boto3
session = boto3.Session(profile_name="subhayan_aws")
ec2 = session.resource('ec2')
