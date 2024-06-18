import logging
import networkx as nx
from networkx.readwrite import json_graph
import json
import re
from operator import itemgetter
import os
import boto3
import base64
from PyFunctionAlgoDictionary import ConnectionMethods
from datetime import datetime, timedelta
import pandas as pd

def MessagingAndRecommendation(event, context):
    conn = ConnectionMethods.getDBConnection()

 ## Accessing the blob
    # connect_str = os.getenv('AzureStorageConnection')
    container_name="tesseranalytics"
    Access_key =  os.getenv("Access_Key")
    Secret_key = os.getenv("Secret_Key")
    region_Name = os.getenv("Region")
    
    try:
        # req_body = req.get_json()

        crsr=conn.cursor()


        g=nx.DiGraph()
        MessageJSON={}
        Recommendations=[]
        Notify=''
        ImpForMyActivity=''
        v_notificationtext=""
        #v_myactivitytext=''
        v_myactivitytext=""

        def replaceMultiple(mainString, toBeReplaces, newString):
            # Iterate over the strings to be replaced
            mainString = mainString.replace(toBeReplaces, newString or "")
            return  mainString


        def space_to_camel(word):
            interim = ''.join(x.capitalize()  for x in word.split(' '))
            op = interim[0].lower()+interim[1:]
            return op

        def transformLogic(input_json,message,output_dict,outcome):
            metadata={}
            metadatabase={}
            metadatalist=[]
            sourceTableStrFrom=' from source '
            targetTableStrTo=' to target'
            sourceTableStr=''
            transformMessageStr=''
            targetTableStr=''
            for key, value in input_json.items():
                if key=='sourceObjects':
                    for i in range(0,int(req_body['sourceTableCount'])):
                        value[i].update({"actionId":"uniq-"+str(len(metadatalist))})
                        sourceTableStr = ' $'+str(value[i]['actionId']+'$ '+sourceTableStr)
                        if value[i]['objectType']=='TABLE':
                            value[i].update({"action":"PREVIEW_TABLE"})
                        elif value[i]['objectType']=='FILE':
                            value[i].update({"action":"PREVIEW_FILE"})
                        metadatalist.append(value[i])
                else:
                    value.update({"actionId":"uniq-"+str(len(metadatalist))})
                    if value['objectType']=='TABLE':
                        value.update({"action":"PREVIEW_TABLE"})
                        targetTableStr =  ' $'+str(value['actionId']+'$ ')
                        value.update({"idForCatalogReco":value['tableId']})
                    elif value['objectType']=='FILE':
                        value.update({"action":"PREVIEW_FILE"})
                        targetTableStr =  ' $'+str(value['actionId']+'$ ')
                    elif value['objectType']=='TRANSFORM':
                        if node=='DeleteTransform' or outcome=='Failure':
                            value.update({"action":"VIEW_TRANSFORM_NO_ACTION"})
                        else:
                            value.update({"action":"VIEW_TRANSFORM"})
                            transformMessageStr = ' $'+str(value['actionId']+'$ ')
                    elif value['objectType']=='CLEANSE':
                        if node=='DeleteCleanse' or outcome=='Failure':
                            value.update({"action":"VIEW_CLEANSE_NO_ACTION"})
                        else:
                            value.update({"action":"VIEW_CLEANSE"})
                            transformMessageStr = ' $'+str(value['actionId']+'$ ')

                    metadatalist.append(value)
            metadata["metadataList"] = metadatalist
            message = str(message) + str(transformMessageStr) + sourceTableStrFrom +  str(sourceTableStr) + targetTableStrTo + str(targetTableStr)
            if outcome == 'Success':
                metadata["message"] =message
            else:
                metadata["message"] = message
                metadata["errorMessage"] = '<errorMessage>'
            metadatabase["metadata"]=metadata
            return (metadatabase)

        if event['isBase64Encoded']:
            req_body = json.loads(base64.b64decode(event['body']).decode('utf-8'))
        else:
            req_body = json.loads(event['body'])
        
        UserID=req_body.get('userId')
        node=req_body.get('eventName')
        outcome=req_body.get('outcome')
        role=space_to_camel(req_body.get('roleName'))
        UserEmail=req_body.get('userEmail')
        FileExtension=req_body.get('fileExtension')
        useridQuery='select [AppAdmin].[ti_adm_getuserid_fn](\''+UserEmail+'\') as userid'
        userDF = pd.read_sql_query(useridQuery,conn)
        userid=userDF['userid'].iloc[0]
        crsr.execute('select TOP 1 OrganizationID from AppAdmin.ti_adm_User_lu where UserEmail = %s and IsActive = 1',UserEmail)
        OrganizationID = crsr.fetchall()
        if not OrganizationID:
            raise ValueError('OrganizationID not available for the user')
        OrganizationID = OrganizationID[0][0]
        #logging.info(role)
        blob_name_eventpath= 'EventLists/'+f'EventList_{str(OrganizationID)}'+'.json'
        # blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        # blob_client_eventpath = blob_service_client.get_blob_client(container=container_name, blob=blob_name_eventpath)
        # eventpathblob=blob_client_eventpath.download_blob()
        s3 = boto3.resource(service_name = 's3', region_name = region_Name, aws_access_key_id = Access_key, aws_secret_access_key = Secret_key)
        eventpathblob = s3.Bucket('spoggledev').Object(container_name + '/' + str(blob_name_eventpath)).get()['Body'].read()
        g=json_graph.node_link_graph(json.loads(eventpathblob),directed=True, multigraph=False)

        transform_json = req_body.pop('transformTables')
        #Notification
        if g.nodes[node]['notify']=='Y':
            Notify=1
            v_notificationtext=''
            if outcome == 'Success':
                v_notificationtext=g.nodes[node]['successNotificationText']
            else:
                v_notificationtext=g.nodes[node]['failureNotificationText']

            if req_body['sourceTableCount']!='':
                v_notificationdict={}
                v_notificationdict=transformLogic(transform_json,v_notificationtext,v_notificationdict,outcome)
                v_notificationtext=''
                v_notificationtext = str(json.dumps(v_notificationdict))

            for key, value in req_body.items():
            #  if value!='':
                    v_notificationtext=replaceMultiple(v_notificationtext,"<"+key+">",value)
            MessageJSON['notificationText']=json.loads(v_notificationtext,strict=False)
        else:
            Notify=0

        #Recommendation
        if outcome == 'Success':
            if node!='IngestFile' or ((node=='IngestFile' or node=='CopyFile') and (FileExtension=='CSV' or FileExtension=='XLSX' or FileExtension=='XLS' or FileExtension=='TXT' )  ):
                nexteventlist = list(g.successors(node))
                nextevent={i for i in nexteventlist}
                subset = g.subgraph(nextevent)
                dict_graph=dict(subset.nodes)
                recommendedevents={}
                for key,value in dict_graph.items():
                    recommendedevent={}
                    for x,y in value.items():
                        if x=='weights' and y!=' ':
                            recommendedevent.update({x:y})
                            recommendedevents.update({key:recommendedevent})
                reco = sorted(recommendedevents,key=itemgetter(1),reverse=False)[:2]
                recommendedsubset = g.subgraph(reco)
                nextnodes = nx.get_node_attributes(recommendedsubset,role)
                if transform_json['transformObject']!=None:
                    if transform_json['targetObject']['objectType']=='FILE':
                        del nextnodes['AnalyzeDataPageLoad']
                    elif transform_json['targetObject']['objectType']=='TABLE':
                        try:
                            del nextnodes['Converttotable']
                        except  KeyError:
                                pass
                    else:
                        pass
                neededforReco = nx.get_node_attributes(recommendedsubset,'impForReco')
                if (nextnodes):
                    for i in nextnodes:
                        for j in neededforReco:
                                if nextnodes[i] == 1:
                                    if i==j:
                                        if neededforReco[j]=='Y':
                                            Recommendation={}
                                            Recommendation['nextEventRecommendation'] = i
                                            if i!="":
                                                RecommendationRaw = g.nodes[j]['recommendationText']
                                                if req_body['sourceTableCount']=='':
                                                    for key, value in req_body.items():
                                                        #if value1!="":
                                                        RecommendationRaw=replaceMultiple(RecommendationRaw,"<"+key+">",value)
                                                        Recommendation['recommendationText'] = json.loads(RecommendationRaw)
                                                    Recommendations.append(Recommendation)
                                                else:
                                                    for key, value in transform_json.items():
                                                        if key=='targetObject':
                                                            for key1,value1 in value.items():

                                                             #  if (nextnodes['Converttotable']==True and transform_json['targetObject']['objectType']=='FILE') or (nextnodes['AnalyzeDataPageLoad']==True and transform_json['targetObject']['objectType']=='TABLE'):
                                                            #   print(x,y)
                                                                RecommendationRaw=replaceMultiple(RecommendationRaw,"<"+key1+">",str(value1))
                                                                Recommendation['recommendationText'] = json.loads(RecommendationRaw)



                                                            Recommendations.append(Recommendation)
                                                        else:
                                                            pass

                                        else:
                                            pass



        MessageJSON['recommendations']=Recommendations


        #MyActivity
    #    v_myactivitytext=""
        if g.nodes[node]['impForMyActivity']=='Y':
            if outcome=='Success':
                v_myactivitytext=g.nodes[node]['myActivityText']
                ImpForMyActivity=1
                idForCatalogReco=''
                for key, value in req_body.items():
                    if value!='':
                        if key=='tableId':
                            idForCatalogReco=value
                        v_myactivitytext=replaceMultiple(v_myactivitytext,"<"+key+">",value)
                if idForCatalogReco!='':
                    v_myactivitytext=v_myactivitytext.replace("<idForCatalogReco>",idForCatalogReco)
                if req_body['sourceTableCount']!='':
                    v_myactivitydict={}
                    v_myactivitydict=transformLogic(transform_json,v_myactivitytext,v_myactivitydict,outcome)
                    v_myactivitytext=''
                    v_myactivitytext = str(json.dumps(v_myactivitydict))
                MessageJSON['myActivityText']=json.loads(v_myactivitytext)
            else:
                ImpForMyActivity=0
        else:
            ImpForMyActivity=0
        crsr.execute("EXEC AppAdmin.ti_adm_EventMessage_Insert_Sp @UserID = ? ,@UserEmail = ?,@Event = ?,@Outcome = ?,@Notify = ?,@ImpForMyActivity = ?,@MessageSent = ?,@MessageRead = ?,@MessageJSON = ?,@CreatedBy= ?,@LastUpdatedBy = ?", UserID,UserEmail,node,outcome,Notify,ImpForMyActivity,0,0,json.dumps(MessageJSON),userid.item(),userid.item())
        crsr.commit()

    except Exception as e :
        ErrorJSON = {}        
        ErrorJSON["status_msg"]="Input parameters not given"
        ErrorJSON["status_code"]=400
        return json.dumps(ErrorJSON)
    
   
    # return func.HttpResponse("success",
    #     status_code=200
    #     )
    return {
        'statusCode': 200,
        'body': json.dumps('success')
    }