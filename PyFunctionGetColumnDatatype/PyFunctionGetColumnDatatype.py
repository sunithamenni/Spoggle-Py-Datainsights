import logging
import json
import os
from datetime import datetime, timedelta
import pandas as pd
from urllib import parse
from collections import OrderedDict
import boto3
from io import BytesIO
import base64
from PyFunctionAlgoDictionary import ConnectionMethods

#from azure.storage.filedatalake import DataLakeServiceClient

inputDF = pd.DataFrame()

#function to check if the datatype is Int or bigint
def checkbigint(inputDF, keyval):
    bigintcolsh = pd.DataFrame()
    bigintcolsl = pd.DataFrame()
    dttype=''
    bigintcolsh = (inputDF[inputDF[keyval] > 2147483647])
    bigintcolsl =  (inputDF[inputDF[keyval] < -2147483647] )
    if  not(bigintcolsh.empty) or not(bigintcolsl.empty):
        dttype='bigint'
    else:
        if inputDF[keyval].isin([0, 1]).all() ==True:
            dttype='bit'
        else:
            dttype='int'
    return  dttype

def try_parsing_date(text,df_datefmt):
    for fmt in df_datefmt:
        try:
            parsed_date = pd.to_datetime(text, format=fmt, errors='raise', exact=True, dayfirst=True)
            # if parsed_date.dt.strftime(fmt)[0] == text[0]:

 
            for row in range(len(parsed_date)): #In case first n rows are null value.
                if pd.isnull(parsed_date[row]):
                    pass
                else:
                    parsed_date_str = parsed_date.dt.strftime(fmt)[row].replace(u'\xa0', ' ')
                    text_str = text[row]
                    parsed_date_obj = pd.to_datetime(parsed_date_str, format=fmt)
                    text_date_obj = pd.to_datetime(text_str, format=fmt) #Fix lack of leading zeroes which pd.datetime automatically puts in parsed_date
                    if parsed_date_obj == text_date_obj: # Check for an exact match
                        return parsed_date
           
        except ValueError:
            pass
    raise ValueError('no valid date format found')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def GetColumnDatatype(event, context):
    try:
        logging.info('Python HTTP trigger function processed a request.')

    ########## connection details start##############################


        ## Accessing the bucket
        # Access_key = os.getenv("Access_Key")
        # Secret_key = os.getenv("Secret_Key")
        # region_Name = os.getenv("Region")
        # logger.info("Environment variables retrieved successfully")
        
        if event['isBase64Encoded']:
            req_body = json.loads(base64.b64decode(event['body']).decode('utf-8'))
        else:
            req_body = json.loads(event['body'])
        # req_body = event['body']
        
        container_name = 'tesseranalytics'
        p_container_name=req_body.get('Location')
        p_blob_file_name=req_body.get('selFileName')
        p_sep = req_body.get('sep') #fetch the file delimiter
        
        # s3 = boto3.resource(service_name = 's3', region_name = region_Name, aws_access_key_id = Access_key, aws_secret_access_key = Secret_key)
        # response = s3.Bucket('spoggledev').Object(str(p_container_name) + '/' + p_blob_file_name).get()['Body']
        # ingestedFileData = pd.read_csv(response, index_col = 0)
        
        ingestedFileData = ConnectionMethods.fileConnectionFetchSubset(p_container_name, p_blob_file_name, p_sep)

        #for getting the standard dateformats
        blob_name_eventpath_date="DateFormat.xlsx"

        #for getting the standard dateformats
        # response2 = s3.Bucket('spoggledev').Object(container_name + '/' + str(blob_name_eventpath_date)).get()['Body'].read()
        # dateFormatData = pd.read_excel(BytesIO(response2))
        dateFormatData = ConnectionMethods.fileConnection(container_name, blob_name_eventpath_date, 'XLSX')


        columnsJSON = {}
    #  ingestedFileData= pd.read_csv(blob_ingestedfile_url_with_sas)
        ingestedFileDatasubset=ingestedFileData.head(100)
        #ingestedJson = ingestedFileData.to_json(orient='records')
    #    ingestedFileDatasubsetJson = ingestedFileDatasubset.to_json(orient='records')
    #    columnsJSON['TableData']=json.loads(ingestedFileDatasubsetJson)
        ingestedFileDatasubsetJson1= ingestedFileDatasubset.assign(**ingestedFileDatasubset.select_dtypes(['datetime']).astype(str).replace({'NaT': None}))
        ingestedFileDatasubsetJson2=ingestedFileDatasubsetJson1.to_json(orient='records')
        columnsJSON['TableData']=json.loads(ingestedFileDatasubsetJson2)

        #check for the Date datatype thru formating  as python returns object datatype for dates

        ingestedFileData_raw = ingestedFileData
        ingestedFileDataRaw=ingestedFileDatasubset
        ingestedFileData = ingestedFileDatasubset.apply(lambda col: pd.to_datetime(col, errors='ignore')
                if col.dtypes == object and pd.isnull(col).all()!=True#Bug Fix-7116(columndatatype not picked up correctly if col has NULLs)
                else pd.to_numeric(col)
                if col.dtypes == object and pd.isnull(col).all()==True
                else col,
                axis=0)
        #derive the column datatype
        ingestedFileColumns=ingestedFileData.dtypes
        logging.info(' after derive default datatypes')

        ColumnsTypeList=[]
        #Check for the column type  for each column - against each of the generic datatypes. the values returned to should match
        # exactly with the data in the LU_GENERIC_COLUMN_DATATYPES and the drop down in the UI when metadata is shown
        for key,value in ingestedFileColumns.items():
            ColumnsType={}
            ColumnsType['ColumnName']=key
            if value.name =='int64':
                # Python return int for bigint and int. So checking from the data if the datatype qualifies for bigint
                ColumnsType['columnType'] = checkbigint(ingestedFileData_raw, key)
            elif value.name =='float64':
                #Python returns float datatype when there are null values in a column. Hence defaulting that to nvarchar
                if ingestedFileData[key].isnull().values.any()==True:
                    ColumnsType['columnType']='nvarchar'
                else:
                    try:
                        #Python returns float for a nullable integer. So explicitly checking for a nullable integer datatype by formating
                        ingestedFileData[key]=ingestedFileData[key].astype('Int64')
                        ColumnsType['columnType']=checkbigint(ingestedFileData, key)
                    except:
                        #if not nullable integer, it is a genuine decimal
                        ColumnsType['columnType']='decimal'
            elif value.name =='bool':
                ColumnsType['columnType']='bit'
            elif (value.name =='datetime64[ns]') or (value.name =='object'):
                try:
                    try_parsing_date(ingestedFileDataRaw[key], dateFormatData['Python'])
                    ColumnsType['columnType']='datetime'
                except ValueError:
                    ColumnsType['columnType']='nvarchar'
                except TypeError:
                    ColumnsType['columnType']='nvarchar'

        #    elif value.name =='object':
        #        ColumnsType['columnType']='nvarchar'
            else :
                ColumnsType['columnType']='nvarchar'
            #create a list with the columntype for each column
            ColumnsTypeList.append(ColumnsType)

    ## earlier functionality of finding datetime datatype
    ##    mask = ingestedFileData.astype(str).apply(lambda x : x.str.match(r'(\d{2,4}-\d{2}-\d{2,4})+').all())
    ##    ingestedFileData.loc[:,mask] = ingestedFileData.loc[:,mask].apply(pd.to_datetime)
    ##    ingestedFileColumns=ingestedFileData.infer_objects().dtypes
    ##    ColumnsTypeList=[]
    ##    for key,value in ingestedFileColumns.items():
    ##        ColumnsType={}
    ##        ColumnsType['ColumnName']=key
    ##        if value.name =='int64':
    ##            ColumnsType['columnType']='int'
    ##        elif value.name =='float64':
    ##            ColumnsType['columnType']='decimal'
    ##        elif value.name =='bool':
    ##            ColumnsType['columnType']='bit'
    ##        elif value.name =='datetime64[ns]':
    ##            ColumnsType['columnType']='datetime'
    ##        elif value.name =='object':
    ##            ColumnsType['columnType']='nvarchar'
    ##        else :
    ##            ColumnsType['columnType']='nvarchar'
    ##        ColumnsType['IsMasked']=0
    ##        ColumnsTypeList.append(ColumnsType)

        ingestedColumnsJson = json.dumps(ColumnsTypeList)
        logging.info(' After deriving the correct datatypes')
        #Append the columntype to output JSON
        columnsJSON['ColumnLists'] = json.loads(ingestedColumnsJson)

        return json.dumps(columnsJSON)
    except Exception as e:
        MessageJSON=str(e)
        MessageJSON=MessageJSON.lstrip()
        # statusCode=[]
        ErrorJSON = {}
        message=[]
        statusCode=400
        ErrorJSON["message"]=MessageJSON
        ErrorJSON["statusCode"]=statusCode
        return json.dumps(ErrorJSON)

if __name__ == "__main__":
    body_json = json.dumps({
  "selFileName": "BillDetails05.csv",
  "FileExt": "CSV",
  "FileDelimiterName": ",",
  "Location": "tesserinsights-com/dilip"
})
    GetColumnDatatype(event = {"body": body_json, "isBase64Encoded": False}, context='')