import boto3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def deploy(input_path, bucket_name, output_key):
    logging.info("Deploying model to S3")
    
    # Create a session using your AWS credentials
    session = boto3.Session(
        aws_access_key_id='',
        aws_secret_access_key=''
    )

    # Create an S3 client
    s3_client = session.client('s3')

    # Upload the model file to the specified S3 bucket
    s3_client.upload_file(input_path, bucket_name, output_key)
    
    return True
