import boto3
from botocore.exceptions import ClientError
import json
import base64
import logging
import os
from sqlalchemy import create_engine
import pandas as pd
import csv
from charset_normalizer import detect
from io import BytesIO
import pickle
import s3fs

logger = logging.getLogger(__name__)

access_key = os.getenv("Access_Key")
secret_key = os.getenv("Secret_Key")
secret_name = os.getenv("Secret_Name")
region_name = os.getenv("Region")
server = os.getenv("Server")
database = os.getenv("Database")

def get_secret():
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
        aws_access_key_id=access_key,aws_secret_access_key=secret_key
    )
    logger.info("RDS Client successfully authenticated.")


    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.


    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        logger.info("RDS Secret Retrieved Successfully")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        else:
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            # decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(secret)
        
def getDBConnection():
    connStr=get_secret()
    logger.info("Secret received")
    
    username = connStr['username'] 
    password = connStr['password']
    
    logger.info("Server name received")
    
    connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}"
    
    # cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';PORT=1433;DATABASE='+database+';uid='+username+';pwd='+ password)
    # connection = pymssql.connect(
    #     server=server,
    #     port=1433,
    #     user=username,
    #     password=password,
    #     database=database
    # )
    conn = create_engine(connection_string)
    logger.info("Connection Received.")
    return conn.raw_connection()

def fileConnection(container_name, blob_name_path, sep='Others'):
    s3 = boto3.resource(service_name = 's3', region_name = region_name, aws_access_key_id = access_key, aws_secret_access_key = secret_key)
    file = s3.Bucket('spoggledev').Object(container_name + '/' + str(blob_name_path)).get()['Body'].read()
    blob_client_path = f's3://spoggledev/{container_name}/{blob_name_path}'
    logger.info('connected to s3')
    if sep is None:
        sep=""

    if sep.upper()=='OTHERS':
        blob_client_path
    else:
        ext=blob_name_path.split('.')[1]
        if ext.upper()=='CSV' or ext.upper()=='TXT' :
            filedata=pd.read_csv(BytesIO(file), sep=sep,keep_default_na = False, na_values = [''])
            return filedata
        # if file is XLS or XLSX
        elif ext.upper()=='XLSX' or ext.upper()=='XLS':
            filedata=pd.read_excel(BytesIO(file))
            return filedata
        elif ext.upper()=='PKL':
            # p = blob_client_path.download_blob()
            filedata = pickle.loads(BytesIO(file))
            return filedata
        elif ext.upper()=='JSON':
            filedata=json.loads(file)
            return filedata
        else:
            return blob_client_path
    # try:
    #     file = s3.Bucket('spoggledev').Object(container_name + '/' + str(blob_name_path)).get()['Body'].read()
    # except ClientError as err:
    #     if err.response['Error']['Code'] == 'NoSuchKey':
    #         statusCode = 500
    #         error_json = {}
    #         error_json["message"] = f"No file called {blob_name_path} was found."
    #         return None, statusCode, error_json
        
    # return file

def writeToBLOB(obj, container_name, blob_name_path):
    s3 = boto3.client('s3',region_name=region_name,aws_access_key_id = access_key, aws_secret_access_key = secret_key)            
    s3.put_object(
        Body=obj,
        Bucket='spoggledev',
        Key=container_name+ '/' + blob_name_path
    )
    return

def fileConnectionFetchSubset(container_name,p_blob_file_name,p_sep):
    s3 = boto3.resource(service_name = 's3', region_name = region_name, aws_access_key_id = access_key, aws_secret_access_key = secret_key)
    file = s3.Bucket('spoggledev').Object(container_name + '/' + str(p_blob_file_name)).get()['Body'].read()
    blob_client_path = f's3://spoggledev/{container_name}/{p_blob_file_name}'
    logger.info('connected to s3')
    
    decode_data = file.decode('utf-8',errors ='ignore')
    lines = decode_data.splitlines()
    head = lines[0] if len(lines)>0 else ''
    delimiter = csv.Sniffer().sniff(head).delimiter
    
    sep = delimiter
    if sep is None:
        sep=""        

    if sep.upper()=='OTHERS':
        blob_client_path
    else:
        ext=p_blob_file_name.split('.')[1]

        if ext.upper()=='CSV' or ext.upper()=='TXT' :
            result = detect(file)
            if result['encoding'] is None:
                result['encoding']='latin-1'
            
            #Check if delimiter is one of the common delimiters:
            delimiter_list = [',', '\t', ';', '|', ' ', ':']
            if sep in delimiter_list:
                pass
            else:
                #Checking the length of columns to appropriately apply delimiter (single columns don't need a delimiter)
                csv_reader = csv.reader(decode_data.content_as_text().splitlines()) # Read the CSV data from the decoded string
                header_row = next(csv_reader) # Get the first row (header) to determine the number of columns
                num_columns = len(header_row) # Count the number of columns
                if num_columns == 1:
                    #No seperater is required for a dataset with 1 column
                    filedata_chunks =pd.read_csv(BytesIO(file),keep_default_na = False, delim_whitespace=True, na_values = [''],nrows=1000,chunksize=1000,encoding=result['encoding'])
                    filedata = pd.concat(filedata_chunks)
                    return filedata
                #In case of custome delimiters and other characters like hyphens, underscores, etc.:
                else:
                    filedata_chunks=pd.read_csv(BytesIO(file), sep=sep,keep_default_na = False, na_values = [''],nrows=1000,chunksize=1000,encoding=result['encoding'])
                    filedata = pd.concat(filedata_chunks)
                    return filedata
            
            # byt=blob_client_path.download_blob().content_as_bytes()
            # The function - content_as_bytes() - has deprecated. readall() function should be used instead:
            # https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.storagestreamdownloader?view=azure-python


            try:
                logger.info('Loading file data')
                logger.info('file path = %s', blob_client_path)
                filedata_chunks=pd.read_csv(BytesIO(file), sep=sep,keep_default_na = False, na_values = [''],nrows=5000,chunksize=1000,encoding=result['encoding'])
                logger.info('file data loaded')
            except UnicodeDecodeError: #In case ascii or utf-8 decoding encounters an unrecognizable character, then use latin-1 decoding:
                filedata_chunks=pd.read_csv(BytesIO(file), sep=sep,keep_default_na = False, na_values = [''],nrows=5000,chunksize=1000,encoding='latin-1')
            
            # chunk_list=[]
            # for chunk in filedata:
            #     chunk_list.append(chunk)
            # concat the list into dataframe
            filedata = pd.concat(filedata_chunks)
            return filedata

        # if file is XLS or XLSX
        elif ext.upper()=='XLSX' or ext.upper()=='XLS':
            filedata=pd.read_excel(BytesIO(file), nrows=1000)
            return filedata
        else:
            return blob_client_path
    
    