import logging
import json
import uuid

def AutoJoin(event,context):
    try:
        
        logging.info('AWS Lambda function processed a request.')
        if event['isBase64Encoded']:
            req_body = json.loads(base64.b64decode(event['body']).decode('utf-8'))
        else:
            req_body = json.loads(event['body'])
        # req_body = event['body']
        tables=req_body.get("tables") 
        print(tables)
        table1_cols=[val.upper() for val in tables[0]["columns"]]
        table2_cols=[val.upper() for val in tables[1]["columns"]]
        common_cols=list(set(table1_cols)&set(table2_cols))
        join={}
        join["joinType"]="innerjoin"
        join["table1"]= {"name": tables[0]["tableNm"],"schemaId": tables[0]["schemaId"],"schemaName": tables[0]["schemaNm"], "alias":tables[0]["alias"], "renderKey":tables[0]["renderKey"]}
        join["table2"]= {"name": tables[1]["tableNm"],"schemaId": tables[1]["schemaId"],"schemaName": tables[1]["schemaNm"], "alias":tables[1]["alias"], "renderKey":tables[1]["renderKey"]}
        columns=[]
        for col in common_cols:
            col1=tables[0]["columns"][table1_cols.index(col)]
            col2=tables[1]["columns"][table2_cols.index(col)]
            columns.append({"column1":col1,"column2":col2,"renderKey":str(uuid.uuid4())[-9:],"operatorkey":"="})
        print(columns)
        if len(columns)==0:
            columns.append({"column1":"","column2":"","renderKey":str(uuid.uuid4())[-9:],"operatorkey":"="})
        join["columns"]=columns
        print('line 27')
        join["renderKey"]=str(uuid.uuid4())[-9:]
        print('line 29')
        return json.dumps(join)
    except Exception as e:
        MessageJSON=str(e)
        MessageJSON=MessageJSON.lstrip()
        ErrorJSON = {}
        message=[]
        statusCode=400 
        ErrorJSON["message"]=MessageJSON
        ErrorJSON["statusCode"]=statusCode    
        return json.dumps(ErrorJSON)
    