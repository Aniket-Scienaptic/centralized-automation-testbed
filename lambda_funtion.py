import json
import boto3

s3_input_bucket = 'testbed-input-data'
s3_output_bucket = 'testbed-output-stream'

def lambda_handler(event, context):
    event['input_file'] = event['inputFile']
    event['s3_output_bucket'] = s3_output_bucket
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=json.dumps(event), Bucket=s3_input_bucket, Key="input.json")
    print("Input file created.")
    client = boto3.client('ecs')
    reponse = client.run_task(
        cluster='testbed-lambda-processor-cluster',
        launchType='FARGATE',
        taskDefinition='testbed-lambda-processor-td:2',
        count=1,
        platformVersion='LATEST',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [
                    'subnet-00c3f3253f9f8e7c0',
                    'subnet-007cbe654e527a0ca',
                ],
                'assignPublicIp': 'DISABLED'
            }
        }
    )
    print("Input request sent to ECS.")
    return {
        'statusCode': 200,
        'body': 'Lambda invoked'
    }