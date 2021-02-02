import boto3
import json
import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config['CLUSTER']['DB_NAME']
DWH_DB_USER = config['CLUSTER']['DB_USER']
DWH_DB_PASSWORD = config['CLUSTER']['DB_PASSWORD']
DWH_PORT = config['CLUSTER']['DB_PORT']

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

ec2 = boto3.client('ec2', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
s3 = boto3.resource('s3', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
iam = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
redshift = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

try:
    print("1.1 Creating a new IAM Role")
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )
#     https://stackoverflow.com/questions/33068055/how-to-handle-errors-with-boto3
except iam.exceptions.EntityAlreadyExistsException as e:
    print('IAM Role already exists')
except Exception as e:
    print("Unexpected error: %s" % e)

print("1.2 Attaching Policy")
res = iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                             PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                             )['ResponseMetadata']['HTTPStatusCode']

print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
print(roleArn)

try:
    response = redshift.create_cluster(
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,

        #Roles (for s3 access)
        IamRoles=[roleArn]
    )
except Exception as e:
    print(e)

response = redshift.describe_clusters()
print(json.dumps(response, indent=4, sort_keys=True, default=str))
