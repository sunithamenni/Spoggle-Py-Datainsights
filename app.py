from PyFunctionGetColumnDatatype.PyFunctionGetColumnDatatype import GetColumnDatatype
from PyFunctionGetMyActivity.PyFunctionGetMyActivity import GetMyActivity
from PyFunctionMessagingAndRecommendation.pyFunctionMessagingAndRecommendation import MessagingAndRecommendation
from PyFunctionUpdateDependencyGraph.PyFunctionUpdateDependencyGraph import UpdateDependencyGraph
from PyFunctionAutoJoin.PyFunctionAutoJoin import AutoJoin
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        path = event.get("requestContext")["http"]["path"]
        # path = event.get("path")
        
        logging.info(f"event is {json.dumps(event)}")
        print(f"The path is {path}")
        
        if path == '/Development/pydatainsights/api/PyFunctionGetColumnDatatype':
            return GetColumnDatatype(event, context)
        elif path == '/Development/pydatainsights/api/PyFunctionGetMyActivity':
            return GetMyActivity(event, context)
        elif path == '/Development/pydatainsights/api/PyFunctionMessagingAndRecommendation':
            return MessagingAndRecommendation(event, context)
        elif path == '/Development/pydatainsights/api/PyFunctionUpdateDependencyGraph':
            return UpdateDependencyGraph(event, context)
        elif path =='/Development/pydatainsights/api/PyFunctionAutoJoin':
            return AutoJoin(event,context)
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
        # return {
        #     'statusCode': 404,
        #     'body': f"Not found. Please check the payload. The event is {json.dumps(event)}"
        # }