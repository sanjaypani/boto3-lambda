import json
import boto3

s3_client = boto3.client('s3')

db_client = boto3.resource('dynamodb')

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    json_file_name = event['Records'][0]['s3']['object']['key']
    json_object = s3_client.get_object(Bucket=bucket_name,Key=json_file_name)
    json_file_reader = json_object['Body'].read()
    json_Dict = json.loads(json_file_reader)
    print(json_Dict)
    
    print("Type:", type(json_Dict))
    
    db_tabel = db_client.Table('employees')
    
    for data in json_Dict:
        emp_id = int(data['emp_id'])
        name = data['name']
        age = data['age']
        print("Adding employee:", emp_id, name, age)
        db_tabel.put_item(Item=data)
    
    return 'Success'
