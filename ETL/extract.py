# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 10:08:19 2023

@author: ayusman
"""

import pandas as pd
import boto3

def extract(bucket, filename, AWS_KEY_ID, AWS_SECRET, region):
    """
    Extracts csv file from an Amazon S3 bucket using the provided credentials and region.

    Parameters:
        bucket (str): The name of the S3 bucket from which to extract the data.
        filename (str): The name of the file to be extracted from the bucket.
        aws_key_id (str): The AWS access key ID with necessary permissions to access the S3 bucket.
        aws_secret (str): The AWS secret access key corresponding to the provided access key ID.
        region (str): The AWS region where the S3 bucket is located.

    Returns:
        df: Panda dataframe of the csv file extracted


    Example:
        aws_key_id = 'your_aws_access_key_id'
        aws_secret = 'your_aws_secret_access_key'
        bucket = 'your_bucket_name'
        filename = 'example.csv'
        region = 'us-east-1'
        data = extract_data_from_s3(bucket, filename, aws_key_id, aws_secret, region)
        

    Note:
        Make sure you have the necessary Boto3 and panda installed before using this function.

    """
    
    s3 = boto3.client('s3', 
                      region_name = region,
                      aws_access_key_id=AWS_KEY_ID, 
                      aws_secret_access_key=AWS_SECRET) 
    
    obj = s3.get_object(Bucket=bucket, Key = filename)
    
    df = pd.read_csv(obj['Body'])
    
    return df
    
    