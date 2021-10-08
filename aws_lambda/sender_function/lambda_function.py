import json
import urllib.parse
import boto3
import os  
from google.cloud import storage


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="config.json"

s3 = boto3.client('s3')
gcp_client = storage.Client.from_service_account_json(json_credentials_path='config.json')
gcp_bucket_name = 'multicloud-madness'
gcp_prefix = 'sync-aws/'

def get_file_s3(bucket, key):
    
    try:
        filename = key.split('/')[-1]
        filepath = f'/tmp/{filename}'
        s3.download_file(bucket, key, filepath) # 500mb
        return filename, filepath
        
    except Exception as e:
        print(e)
        print(f'Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function.')
        raise e
        
def send_file_gcp(filename, filepath):
    # Creating bucket object
    bucket = gcp_client.get_bucket(gcp_bucket_name)
    
    # Name of the object to be stored in the bucket
    object_name_in_gcs_bucket = bucket.blob(f'{gcp_prefix}{filename}')
    
    # Name of the object in local file system
    object_name_in_gcs_bucket.upload_from_filename(filepath)

def lambda_handler(event, context):
    
    # Get bucket and key
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    # Get file from s3
    filename, filepath = get_file_s3(bucket, key)
    
    # Send file to gcs
    send_file_gcp(filename, filepath)
    
    return {'message': 'Success'}

    