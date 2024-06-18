import logging
import json
import os
from datetime import datetime, timedelta
from urllib import parse
from collections import OrderedDict
from PyFunctionAlgoDictionary import ConnectionMethods
import boto3
from botocore.exceptions import ClientError
import pandas as pd

# import requests
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def GetMyActivity(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    try:
        Access_key = os.getenv("Access_Key")
        Secret_key = os.getenv("Secret_Key")
        region_Name = os.getenv("Region")
        logger.info("Environment variables retrieved successfully")
        
        if event['isBase64Encoded']:
            req_body = json.loads(base64.b64decode(event['body']).decode('utf-8'))
        else:
            req_body = json.loads(event['body'])
        # req_body = event['body']
        
        s3 = boto3.resource(service_name = 's3', region_name = region_Name, aws_access_key_id = Access_key, aws_secret_access_key = Secret_key)
        logger.info("Connected to S3")
        
        conn = ConnectionMethods.getDBConnection()
        crsr = conn.cursor()
        logger.info("Got DB Connection")
        
        user_email = req_body.get('userEmail')
        firsttime_indicator = req_body.get('firsttimeIndicator')
        last_msg = req_body.get('lastMessage')
        v_called_from=req_body.get('calledFrom')
        # userschema = req_body.get('userschema')
        # usertable = req_body.get('usertable')
        #local_file_name="recommendations.json"
        #container_name="tesseranalytics\Quick Insights"

        container_name="tesseranalytics"
        connect_str = os.getenv('AzureStorageConnection')

        if v_called_from=='L':
            p_top= os.getenv('LandingPageRecoCount')
            p_objectid= ''
        elif v_called_from=='C':
            p_top=os.getenv('CatalogPageRecoCount')
            p_objectid= req_body.get('objectid')
        elif v_called_from=='MO':
            p_top=os.getenv('MyActivityPageObjRecoCount')
            p_objectid= req_body.get('objectid')
        else:
            p_top=os.getenv('MyActivityPageRecoCount')
            p_objectid= ''
        logger.info("Started function")
        
        # Old query
        # if v_called_from=='C':
        #     query = ("""select top """+str(p_top)+""" messageid,event,messagejson, createddate from appadmin.ti_adm_eventmessage
        #             where UserEmail='"""+user_email+"""'
        #             and messageid<"""+str(last_msg)+"""
        #             and messageJSON like '%"idForCatalogReco": """+"\""+str(p_objectid)+"\""+"""%'
        #             and event in ('CopyTable','Converttotable','CreateandRunTransform','CleanseData','EditandRunTransform','EditCleanse','RerunTransform','RerunCleanse')
        #             and ImpForMyActivity=1
        #             and outcome='Success'
        #             order by messageid desc
        #         """)
        # elif v_called_from=='MO':
        #     query = ("""select top """+str(p_top)+""" messageid,event,messagejson, createddate from appadmin.ti_adm_eventmessage
        #             where UserEmail='"""+user_email+"""'
        #             and messageid<"""+str(last_msg)+"""
        #             and messageJSON like '%"""+str(p_objectid)+"""%'
        #             and ImpForMyActivity=1
        #             and outcome='Success'
        #             order by messageid desc
        #         """)
        # else:
        #     query = ("""select top """+str(p_top)+""" messageid,event,messagejson, createddate from appadmin.ti_adm_eventmessage
        #             where UserEmail='"""+user_email+"""'
        #             and messageid<"""+str(last_msg)+"""
        #             and ImpForMyActivity=1
        #             and outcome='Success'
        #             order by messageid desc
        #         """)

        # Modified Sp with all condition
        crsr.execute('EXEC appadmin.ti_adm_GetEventMessagesByCondition_Sp  @p_top = %d, @user_email = %s, @last_msg = %d, @p_objectid = %d, @v_called_from = %s',(p_top,user_email,last_msg,p_objectid,v_called_from))
        # db_events = pd.read_sql_query(query,conn)
        db_events = crsr.fetchall()
        logging.info("db_events %s", db_events)
        no_of_events=len(db_events)
        # Get column names from cursor description
        column_names = [column[0] for column in crsr.description]

        # Create a DataFrame
        db_events = pd.DataFrame.from_records(db_events, columns=column_names)
        crsr.close()
        activity_array=[]
        no_of_events=len(db_events)
        objid_set = set()
        for x in range(no_of_events):
            msg_json=json.loads(db_events.loc[x]["messagejson"]) #msg_json=json.loads(db_events.ix[x]["messagejson"])
            metadata_list = msg_json['myActivityText']['metadata']['metadataList']
            metadata_length = len(metadata_list)
            for i in range(metadata_length):
                try:
                    if str(metadata_list[i]["fileId"]).isnumeric():
                        objid_set.add(str(metadata_list[i]["fileId"]))
                except KeyError:
                    pass
                try:
                    if str(metadata_list[i]["objectId"]).isnumeric():
                        objid_set.add(str(metadata_list[i]["objectId"]))
                except KeyError:
                    pass
                try:
                    if str(metadata_list[i]["tableId"]).isnumeric():
                        objid_set.add(str(metadata_list[i]["tableId"]))
                except KeyError:
                    pass
                try:
                    if str(metadata_list[i]["cleanseId"]).isnumeric():
                        objid_set.add(str(metadata_list[i]["cleanseId"]))
                except KeyError:
                    pass
                try:
                    if str(metadata_list[i]["transformId"]).isnumeric():
                        objid_set.add(str(metadata_list[i]["transformId"]))
                except KeyError:
                    pass
        objid_set.discard("0")
        objid_set.discard("")
        objid_str = ','.join(objid_set)
        if objid_str!='':
            # query = ("""select cast(ObjectID as varchar) as ObjectID, IsActive, TAI_Enabled,isTAIRunning,isUnivariateRunning,PBIDataSetStatus,InProgress from appadmin.ti_adm_ObjectOwner
            #             where ObjectID in ("""+objid_str+""")""")
            # db_objects = pd.read_sql_query(query,conn)
            crsr=conn.cursor()
            crsr.execute('EXEC appadmin.ti_adm_GetObjectOwnerInfo_Sp @objid_str = ?',objid_str)
            db_objects = crsr.fetchall()
            # Get column names from cursor description
            column_names = [column[0] for column in crsr.description]

    # Create a DataFrame
            db_objects = pd.DataFrame.from_records(db_objects, columns=column_names)
            crsr.close()
        for x in range(no_of_events):
            msg=int(db_events.loc[x]["messageid"]) #msg=int(db_events.ix[x]["messageid"])
            timestamp = str(db_events.loc[x]["createddate"]) #timestamp = str(db_events.ix[x]["createddate"])
            if last_msg>msg:
                last_msg=msg
            a=db_events.loc[x]["messagejson"] #a=db_events.ix[x]["messagejson"]
            msg_json=json.loads(db_events.loc[x]["messagejson"]) #msg_json=json.loads(db_events.ix[x]["messagejson"])
            metadata_list = msg_json['myActivityText']['metadata']['metadataList']
            metadata_length = len(metadata_list)
            is_valid = 1
            for i in range(metadata_length):
                try:
                    is_valid = 1
                    # id_parameter = {'objectId','fileId','tableId','cleanseId','transformId'} & set(metadata_list[i].keys())
                    # if not id_parameter:
                    #     raise KeyError
                    # if len(id_parameter) > 1:
                    #     raise IndexError
                #     if (
                #         (db_objects[db_objects["ObjectID"] == str(metadata_list[i][list(id_parameter)[0]])].empty)
                #         or (
                #             not(
                #                 db_objects[db_objects["ObjectID"] == str(metadata_list[i][list(id_parameter)[0]])]['IsActive'].values[0]
                #             )
                #         )
                #     ):
                #         if 'NO_ACTION' not in metadata_list[i]["action"]:
                #             msg_json['myActivityText']['metadata']['metadataList'][i]["action"] = metadata_list[i]["action"] + '_NO_ACTION'
                #         is_valid = 0
                # except KeyError:
                #     pass
                # except IndexError:
                #     pass
                    keys_in_metadata_set = set(metadata_list[i].keys()) & {'fileId','objectId','tableId','cleanseId','transformId'}
                    for key in keys_in_metadata_set:
                        if (metadata_list[i][key] != None) and (metadata_list[i][key] != '') and (str(metadata_list[i][key]) != "0"):
                            if (
                                (db_objects[db_objects["ObjectID"] == str(metadata_list[i][key])].empty)
                                or (
                                    not(
                                        db_objects[db_objects["ObjectID"] == str(metadata_list[i][key])]['IsActive'].values[0]
                                    )
                                )
                            ):
                                if 'NO_ACTION' not in metadata_list[i]["action"]:
                                    msg_json['myActivityText']['metadata']['metadataList'][i]["action"] = metadata_list[i]["action"] + '_NO_ACTION'
                                is_valid = 0

                    # if (
                    #     (db_objects[db_objects["ObjectID"] == str(metadata_list[i]["objectId"])].empty)
                    #     or (
                    #         not(
                    #             db_objects[db_objects["ObjectID"] == str(metadata_list[i]["objectId"])]['IsActive'].values[0]
                    #         )
                    #     )
                    # ) if 'objectId' in metadata_list[i] else (
                    #     (db_objects[db_objects["ObjectID"] == str(metadata_list[i]["fileId"])].empty)
                    #     or (
                    #         not(
                    #             db_objects[db_objects["ObjectID"] == str(metadata_list[i]["fileId"])]['IsActive'].values[0]
                    #         )
                    #     )
                    # ):
                    #     if 'NO_ACTION' not in metadata_list[i]["action"]:
                    #         msg_json['myActivityText']['metadata']['metadataList'][i]["action"] = metadata_list[i]["action"] + '_NO_ACTION'
                    #     is_valid = 0
                except KeyError:
                    pass
                except IndexError:
                    pass
                # try:
                #     if len(db_objects[db_objects["ObjectID"] == str(metadata_list[i]["objectId"])]) == 0 or not(db_objects["IsActive"][db_objects[db_objects["ObjectID"] == str(metadata_list[i]["objectId"])].index.values.astype(int)[0]]):
                #         if 'NO_ACTION' not in metadata_list[i]["action"]:
                #             msg_json['myActivityText']['metadata']['metadataList'][i]["action"] = metadata_list[i]["action"] + '_NO_ACTION'
                #         is_valid = 0
                # except KeyError:
                #     pass
                # ! The following sections also have to be modified.
                try:
                    l = db_objects[db_objects["ObjectID"] == str(metadata_list[i]["tableId"])]
                    if metadata_list[i]["tableId"] and (len(db_objects[db_objects["ObjectID"] == str(metadata_list[i]["tableId"])]) == 0 or not(db_objects["IsActive"][db_objects[db_objects["ObjectID"] == str(metadata_list[i]["tableId"])].index.values.astype(int)[0]])):
                        if 'NO_ACTION' not in metadata_list[i]["action"]:
                            msg_json['myActivityText']['metadata']['metadataList'][i]["action"] = metadata_list[i]["action"] + '_NO_ACTION'
                        metadata_list[i]["objectId"] = metadata_list[i]["tableId"]
                        is_valid = 0
                except KeyError:
                    pass
                try:
                    if metadata_list[i]["objectType"] == "CLEANSE":
                        if len(db_objects[db_objects["ObjectID"] == str(metadata_list[i]["cleanseId"])]) == 0 or not(db_objects["IsActive"][db_objects[db_objects["ObjectID"] == str(metadata_list[i]["cleanseId"])].index.values.astype(int)[0]]):
                            if 'NO_ACTION' not in metadata_list[i]["action"]:
                                msg_json['myActivityText']['metadata']['metadataList'][i]["action"] = metadata_list[i]["action"] + '_NO_ACTION'
                            metadata_list[i]["objectId"] = metadata_list[i]["cleanseId"]
                            is_valid = 0
                except KeyError:
                    pass
                try:
                    if metadata_list[i]["objectType"] == 'TRANSFORM':
                        if len(db_objects[db_objects["ObjectID"] == str(metadata_list[i]["transformId"])]) == 0 or not(db_objects["IsActive"][db_objects[db_objects["ObjectID"] == str(metadata_list[i]["transformId"])].index.values.astype(int)[0]]):
                            if 'NO_ACTION' not in metadata_list[i]["action"]:
                                msg_json['myActivityText']['metadata']['metadataList'][i]["action"] = metadata_list[i]["action"] + '_NO_ACTION'
                            metadata_list[i]["objectId"] = metadata_list[i]["transformId"]
                            is_valid = 0
                except KeyError:
                    pass
            event=db_events.loc[x]["event"] #event=db_events.ix[x]["event"]
            activity={}
            activity["activity"] = event
            activity["timestamp"] = timestamp
            activity["myActivityText"] = msg_json["myActivityText"]
            activity["recommendations"]=[]
            if firsttime_indicator == 'Y' and is_valid == 1:
                number_of_data_reco=0
                # if quick_insights_found == 'Y':
                if event.lower() in ('ingesttable','converttotable','createandruntransform','editandruntransform','reruntransform','cleansedata','editcleanse','reruncleanse'):
                    # metadata_list = msg_json['myActivityText']['metadata']['metadataList']
                    # metadata_length = len(metadata_list)
                    schema_name = metadata_list[metadata_length-1]['schemaName']
                    table_name = metadata_list[metadata_length-1]['tableName']
                    try:
                        local_file_name=f"TAI~Reco~Dataset~{schema_name}~{table_name}~.json"
                        quick_insights_found='Y'
                        try:
                            blb=s3.Bucket('spoggledev').Object(container_name + '/TAI/' + local_file_name).get()['Body'].read()
                        except ClientError as err:
                            if err.response['Error']['Code'] == 'NoSuchKey':
                                quick_insights_found='N'
                        else:
                            data_reco=json.loads(blb)
                        if quick_insights_found == 'Y':
                            d=data_reco[schema_name][table_name]
                    except KeyError:
                        pass
                    else:
                        if quick_insights_found == 'Y':
                            len_d=len(d)
                            number_of_data_reco = 0
                            for i in range(0,len_d):
                                temp_reco=d[len_d-i-1]
                                if temp_reco['recommendationText']['metadata']['metadataList'][0]['action'] in ['REG-MODEL','DATA-QUALITY','RELATIONAL','UNIVARIATE_RECOMMENDATION']:
                                    continue
                                if 'taiEnabled' in temp_reco['recommendationText']['metadata']['metadataList'][0]: #Tai enabled is false should this section still run ?

                                    if (temp_reco['recommendationText']['metadata']['metadataList'][0]['action']=='ANALYZE_TABLE'):
                                        if ((db_objects["isUnivariateRunning"][db_objects[db_objects["ObjectID"] == str(temp_reco['recommendationText']['metadata']['metadataList'][0]["tableId"])].index.values.astype(int)[0]])==0) :
                                            temp_reco['recommendationText']['metadata']['metadataList'][0]['taiEnabled'] = int(db_objects["Tai_Enabled"][db_objects[db_objects["ObjectID"] == str(metadata_list[i]["tableId"])].index.values.astype(int)[0]])
                                    elif (temp_reco['recommendationText']['metadata']['metadataList'][0]['action']=='CREATE_REPORT'):
                                        if ((db_objects["PBIDataSetStatus"][db_objects[db_objects["ObjectID"] == str(temp_reco['recommendationText']['metadata']['metadataList'][0]["tableId"])].index.values.astype(int)[0]])==0) :
                                            temp_reco['recommendationText']['metadata']['metadataList'][0]['taiEnabled'] = int(db_objects["Tai_Enabled"][db_objects[db_objects["ObjectID"] == str(metadata_list[i]["tableId"])].index.values.astype(int)[0]])
                                    elif (temp_reco['recommendationText']['metadata']['metadataList'][0]['action']=='VIEW_INSIGHTS'):
                                        if ((db_objects["isTAIRunning"][db_objects[db_objects["ObjectID"] == str(temp_reco['recommendationText']['metadata']['metadataList'][0]["tableId"])].index.values.astype(int)[0]])==0) :
                                            temp_reco['recommendationText']['metadata']['metadataList'][0]['taiEnabled'] = int(db_objects["Tai_Enabled"][db_objects[db_objects["ObjectID"] == str(metadata_list[i]["tableId"])].index.values.astype(int)[0]])
                                    else:
                                            temp_reco['recommendationText']['metadata']['metadataList'][0]['taiEnabled'] = int(db_objects["Tai_Enabled"][db_objects[db_objects["ObjectID"] == str(metadata_list[i]["tableId"])].index.values.astype(int)[0]])

                                temp_reco["tesserAI"]="Y"
                                activity["recommendations"].append(temp_reco)
                                number_of_data_reco = number_of_data_reco + 1
                                if number_of_data_reco == 2:
                                    break
                number_of_event_reco = 3 - number_of_data_reco
                event_reco = msg_json["recommendations"]
                if number_of_event_reco > len(event_reco):
                    number_of_event_reco=len(event_reco)
                for i in range(0,number_of_event_reco):
                    temp_reco = event_reco[i]
                    if 'taiEnabled' in temp_reco['recommendationText']['metadata']['metadataList'][0]: #Tai enabled is false should this section still run ?
                        if (temp_reco['recommendationText']['metadata']['metadataList'][0]['action']=='ANALYZE_TABLE'):
                            if ((db_objects["isUnivariateRunning"][db_objects[db_objects["ObjectID"] == str(temp_reco['recommendationText']['metadata']['metadataList'][0]["tableId"])].index.values.astype(int)[0]])==0) :
                                temp_reco['recommendationText']['metadata']['metadataList'][0]['taiEnabled'] = int(db_objects["TAI_Enabled"][db_objects[db_objects["ObjectID"] ==  str(temp_reco['recommendationText']['metadata']['metadataList'][0]["tableId"])].index.values.astype(int)[0]])
                        elif (temp_reco['recommendationText']['metadata']['metadataList'][0]['action']=='CREATE_REPORT'):
                            if ((db_objects["PBIDataSetStatus"][db_objects[db_objects["ObjectID"] == str(temp_reco['recommendationText']['metadata']['metadataList'][0]["tableId"])].index.values.astype(int)[0]])==1) :
                                temp_reco['recommendationText']['metadata']['metadataList'][0]['taiEnabled'] = int(db_objects["TAI_Enabled"][db_objects[db_objects["ObjectID"] ==  str(temp_reco['recommendationText']['metadata']['metadataList'][0]["tableId"])].index.values.astype(int)[0]])
                        elif (temp_reco['recommendationText']['metadata']['metadataList'][0]['action']=='VIEW_INSIGHTS'):
                            if ((db_objects["isTAIRunning"][db_objects[db_objects["ObjectID"] == str(temp_reco['recommendationText']['metadata']['metadataList'][0]["tableId"])].index.values.astype(int)[0]])==0) :
                                temp_reco['recommendationText']['metadata']['metadataList'][0]['taiEnabled'] = int(db_objects["TAI_Enabled"][db_objects[db_objects["ObjectID"] ==  str(temp_reco['recommendationText']['metadata']['metadataList'][0]["tableId"])].index.values.astype(int)[0]])
                        else:
                            taiflag = db_objects["TAI_Enabled"][db_objects[db_objects["ObjectID"] ==  str(temp_reco['recommendationText']['metadata']['metadataList'][0]["tableId"])].index.values.astype(int)[0]]
                            temp_reco['recommendationText']['metadata']['metadataList'][0]['taiEnabled'] = int(db_objects["TAI_Enabled"][db_objects[db_objects["ObjectID"] ==  str(temp_reco['recommendationText']['metadata']['metadataList'][0]["tableId"])].index.values.astype(int)[0]])
                    temp_reco["tesserAI"]="N"
                    activity["recommendations"].append(temp_reco)
            activity_array.append(activity)
        if no_of_events<20:
            show_more_indicator='N'
        else:
            show_more_indicator='Y'
        activity_list={}
        activity_list["activities"] = activity_array
        activity_list["lastMessage"] = last_msg
        activity_list["showMoreIndicator"] = show_more_indicator
        activity_list["statusCode"] = 200
        logging.info("Function End")
        return json.dumps(activity_list)
    
    except Exception as e:
        MessageJSON=str(e)
        MessageJSON=MessageJSON.lstrip()
        statuscode=[]
        ErrorJSON = {}
        message=[]
        statusCode=400
        ErrorJSON["message"]=MessageJSON
        ErrorJSON["statusCode"]=statusCode
        return json.dumps(ErrorJSON)
