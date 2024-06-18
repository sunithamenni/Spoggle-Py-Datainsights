import logging
import networkx as nx
from networkx.readwrite import json_graph;
import json
import os
import boto3
import base64

def UpdateDependencyGraph(event, context):
    try:

        #logger = logging.getLogger()
        #logger.setLevel(logging.INFO)        

        logging.info('Python HTTP trigger function processed a request.')
        
        try:
            if event['isBase64Encoded']:
                req_body = json.loads(base64.b64decode(event['body']).decode('utf-8'))
            else:
                req_body = json.loads(event['body'])
                
        except ValueError:
            return_json = {}
            return_json['status_msg'] = "Input parameters not given"
            return_json['status_code'] = 400
            return json.dumps(ErrorJSON)
        else:

        ## Accessing the bucket
            Access_key = os.getenv("Access_Key")
            Secret_key = os.getenv("Secret_Key")
            region_Name = os.getenv("Region")
            logging.info("Environment variables retrieved successfully")
        
            #[RP] connect_str = os.getenv('AzureStorageConnection')

            local_file_name="objDependency.json"
            container_name="tesseranalytics"

            try:
                s3 = boto3.resource(service_name = 's3', region_name = region_Name, aws_access_key_id = Access_key, aws_secret_access_key = Secret_key)
                blb = s3.Bucket('spoggledev').Object(container_name + '/' + str(local_file_name)).get()['Body'].read()

                logging.info(type(blb))
            except:
                g = nx.DiGraph()
            else:
                g=json_graph.node_link_graph(json.loads(blb)) 
  
            logging.info("1")
            action_type = req_body.get('actionType')
            object_owner = req_body.get('owner')
            object_id = req_body.get('objectId')
            object_name = req_body.get('objectName')
            object_location = req_body.get('objectLocation')
            object_type = req_body.get('objectType')
            created_date = req_body.get('createdDate')
            source_objects_count = req_body.get('sourceObjectsCount')
            logging.info("2")
            if source_objects_count is not None and source_objects_count != 0:
                source_objects = req_body.get('sourceObjects')
                dependency_description=req_body.get('dependencyDescription')
            logging.info("3")
            if action_type=='ADD_OBJECT':
                logging.info("4")
                if g.has_node(object_id):
                    g.remove_node(object_id)
                logging.info("6")
                g.add_node(object_id, objectOwner = object_owner, objectName=object_name, objectLocation=object_location, CreatedDate = created_date, objectType=object_type, isActive='y')
                status_msg="Object "+ object_id +" added to object dependency graph"
                if source_objects_count is not None and source_objects_count != 0 and source_objects is not None:
                    soc=source_objects_count-1
                    for i in (0,soc):
                        if not(g.has_node(source_objects[i]["sourceObjectId"])):
                            g.add_node(source_objects[i]["sourceObjectId"], objectName=source_objects[i]["sourceObjectName"], objectLocation=source_objects[i]["sourceObjectLocation"], objectType=source_objects[i]["sourceObjectType"], isActive='y')
                        g.add_edge(source_objects[i]["sourceObjectId"],object_id,dependencyDescription=dependency_description, isActive='y')
                logging.info("7")
            if action_type=='REMOVE_OBJECT':
                if g.has_node(object_id) and g.nodes(data=True)[object_id]['isActive']=='y':
                    nx.set_node_attributes(g,{object_id:'n'},name='isActive')
                    status_msg="Object "+object_id+" removed from object dependency graph"
                else:
                    status_msg="Object "+object_id+" does not exist in object dependency graph"
                    statusJSON = {}        
                    statusCode=400
                    statusJSON["status_msg"]=status_msg
                    statusJSON["status_code"]=statusCode
                    return json.dumps(statusJSON)

            if action_type=='ADD_DEPENDENCY':
                if not(g.has_node(object_id)):
                    g.add_node(object_id, objectOwner = object_owner, objectName=object_name, objectLocation=object_location, CreatedDate = created_date, objectType=object_type, isActive='y')
                if source_objects_count is not None and source_objects_count != 0 and source_objects is not None:
                        soc=source_objects_count-1
                        logging.info(soc)
                        #Changed for i in (0,soc) to range function because range interates over all the values and the 
                        # previous code only iterates over the first and last values.
                        for i in range (source_objects_count):
                            logging.info(source_objects[i]["sourceObjectId"])
                            if not(g.has_node(source_objects[i]["sourceObjectId"])):
                                g.add_node(source_objects[i]["sourceObjectId"], objectName=source_objects[i]["sourceObjectName"], objectLocation=source_objects[i]["sourceObjectLocation"], objectType=source_objects[i]["sourceObjectType"], isActive='y')
                            g.add_edge(source_objects[i]["sourceObjectId"],object_id,dependencyDescription=dependency_description, isActive='y')
                status_msg="Dependency created between objects"
            if action_type=='COPY_DEPENDENCY':
                if not(g.has_node(object_id)):
                    g.add_node(object_id, objectOwner = object_owner, objectName=object_name, objectLocation=object_location, CreatedDate = created_date, objectType=object_type, isActive='y')
                if source_objects_count is not None and source_objects_count != 0 and source_objects is not None:
                        soc=source_objects_count-1
                        logging.info(soc)
                        for i in (0,soc):
                            logging.info(source_objects[i]["sourceObjectId"])
                            if not(g.has_node(source_objects[i]["sourceObjectId"])):
                                g.add_node(source_objects[i]["sourceObjectId"], objectName=source_objects[i]["sourceObjectName"], objectLocation=source_objects[i]["sourceObjectLocation"], objectType=source_objects[i]["sourceObjectType"], isActive='y')
                            ls = list(g.in_edges(source_objects[i]["sourceObjectId"],data=True))
                            for i in ls:
                                g.add_edge(i[0],object_id,dependencyDescription=i[2]['dependencyDescription'], isActive='y')
                status_msg="Dependency copied from source object to target object"
            
            logging.info("8")
            g_reversed=g.reverse(copy=True)
            logging.info("9")
            #[RP] pr = nx.pagerank(g_reversed)
            logging.info("10")    

            s3 = boto3.client('s3',region_name=region_Name,aws_access_key_id = Access_key, aws_secret_access_key = Secret_key)            
            s3.put_object(
                Body=json.dumps(json_graph.node_link_data(g)),
                Bucket='spoggledev',
                Key=container_name+ '/' + local_file_name
            )
            
            logging.info("12")
            return status_msg 
        
    except Exception as e:
        MessageJSON=str(e)
        MessageJSON=MessageJSON.lstrip()        
        ErrorJSON = {}        
        statusCode=400
        ErrorJSON["message"]=MessageJSON
        ErrorJSON["statusCode"]=statusCode
        return json.dumps(ErrorJSON)