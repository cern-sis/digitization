import os

import boto3
from dotenv import load_dotenv

load_dotenv()


def get_s3_file_path(filetype='', box_file='', filename=''):
    box_file = box_file.split('.')[0].upper().replace('-', '_')
    if filetype == 'PDF' or filetype == 'TIFF':
        return f"raw/{filetype}/{box_file}/{filename}/"
    elif filetype == 'PDF_LATEX':
        return f"raw/{filetype}/{box_file}/{filename}.pdf"


def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.environ['ACCESS_KEY'],
        aws_secret_access_key=os.environ['SECRET_KEY'],
        endpoint_url='https://s3.cern.ch',
    )

def list_s3_files(bucket_name, prefix, s3_client=None):
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter='/'
        )
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents'] if 
                    not obj['Key'].endswith('/')]
        else:
            return []
    except Exception:
        return []

def generate_s3_url(bucket_name, file_key, expiration=31556952, s3_client=None):
    return f"{bucket_name}/{file_key}/{expiration}"
    return s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket_name, "Key": file_key},
            ExpiresIn=expiration,
        )
