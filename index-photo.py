import base64
import json
import boto3
import os
import sys
import uuid
from botocore.vendored import requests
# from PIL import Image
# import PIL.Image
from datetime import *
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection
import string
import random
#filer
ES_HOST = 'search-photos-ftjcbzutcrr3guaf6cd4upfafy.us-east-1.es.amazonaws.com'
REGION = 'us-east-1'
INDEX= 'photos'
def lambda_handler(event, context):
    print("EVENT --- {}".format(json.dumps(event)))
    print("context: ",context)
    headers = { "Content-Type": "application/json" }
    rek = boto3.client('rekognition')
    
    N = 7

    picture=event["body"]
    #print("picture raw: ", picture)

    picture=base64.b64decode(picture)
    #print(picture)
    key=''.join(random.choices(string.ascii_uppercase +string.digits, k=N))
    key=key+".jpg"
    bucket="kevinshi-b2"
    print("key: ",key)
    
        
    s3 = boto3.client('s3') #put picture into s3
    s3.put_object(Bucket=bucket, Key=key, Body=picture)
    
    # get the image information from S3
   
        # detect the labels of current image
    labels = rek.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },
        MaxLabels=10
    )
        
    print("IMAGE LABELS --- {}".format(labels['Labels']))
    
    # prepare JSON object
    obj = {}
    obj['objectKey'] = key
    obj["bucket"] = bucket
    obj["createdTimestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    obj["labels"] = []
        
    for label in labels['Labels']: #build custom labels
        obj["labels"].append(label['Name'].lower())
    plural=[x+"s" for x in obj["labels"]]
    obj["labels"]+=plural
    
    custom_label_string=event["headers"]["x-amz-meta-customlabels"]
    if custom_label_string:
        custom_label_list=custom_label_string.split(",")
        obj["labels"]+=custom_label_list
    print("labels: ",obj["labels"])
    
   
   
    object = json.dumps(obj)
    post(object,key)

    return {
        'statusCode': 200,
        'headers': {
             "Access-Control-Allow-Origin": "*",
                'Content-Type': 'application/json',
                "Access-Control-Allow-Headers":"*",
                "Access-Control-Allow-Methods":"*",
        },
        'body': json.dumps("Image labels have been successfully detected!")
    }
def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
def post(document,key):
    awsauth =get_awsauth(REGION,"es")
    client=boto3.client('opensearch')
    es_client=OpenSearch(hosts=[{
            'host' : ES_HOST,
            'port' : 443
    }],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)
    index_body={
        'settings' : {
            'index' : {
                'number_of_shards' : 1
            }
        }
    }

    res=es_client.index(index=INDEX, id=key, body=document)
    print("res: ",res)
    print("more res: ",es_client.get(index=INDEX,id=key))
