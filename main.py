import json
import boto3
import requests
import time
import pandas as pd
import numpy as np
import traceback
import multiprocessing
from pandas import json_normalize
from io import StringIO
from centralize_automation_testbed import pre_process, get_response_from_iris, generate_json_response, custom_attribute, \
    generate_csv_response, get_file_content

s3_input_bucket = 'testbed-input-data'
s3_client = boto3.client('s3')
input_key = "config.json"


def automation_testbed():
    print("Testing started")
    try:
        input_event = s3_client.get_object(Bucket=s3_input_bucket, Key=input_key)['Body'].read().decode('utf-8')
    except Exception as e:
        print("Failed to read configuration,", str(e))
        return
    event = json.loads(input_event)
    print("Input reading finished")
    s3_output_bucket = event['s3_output_bucket']
    input_file = event['input_file']
    los = event['combinations']['los']
    bureau = event['combinations']['bureau']
    evaluatorEndpoint = event['evaluatorEndpoint']
    lowerLimit = event['recordRange']['lowerLimit'] - 1
    upperLimit = event['recordRange']['upperLimit'] - 1
    applicationIds = event['applicationIds']  # list
    customFields = event['customFields']  # list
    metric = event['metric']  # bool True/False
    completeResponse = event['completeResponse']
    authToken = event['authToken']
    clientName = event['clientName']
    flowId = {'FlowId': evaluatorEndpoint.strip().split("/")[-1]}
    # input directory name
    if los and bureau:
        dir_name = los + '&' + bureau
    elif los:
        dir_name = los
    elif bureau:
        dir_name = bureau
    else:
        return "Please enter los and/or bureau"

    if not authToken.startswith('Bearer'):
        authToken = 'Bearer ' + authToken

    headers = {
        'Content-Type': 'application/json',
        'Authorization': authToken
    }
    # input object key path
    object_key = dir_name + '/' + input_file

    total_time = time.time()
    # getting file contents
    try:
        t1 = time.time()
        file_content = get_file_content(s3_input_bucket, object_key, s3_client)
        print("Time taken to get and split data from s3", ((time.time() - t1) / 60), "minute")
    except Exception as e:
        print("Failed to read input data from s3,", str(e))
        return

    if lowerLimit < 0 or lowerLimit > len(file_content) or upperLimit < 0 or upperLimit > len(file_content):
        print("Index out of range. Please enter correct upper and lower limit.")
        return
    # preprocessing data
    try:
        t2 = time.time()
        data = pre_process(clientName, applicationIds, lowerLimit, upperLimit, file_content)
        dataLength = len(data)
        print("Time taken to load data from file_content", ((time.time() - t2) / 60), "minute")
        print("Number of applicant data loaded from json:", dataLength)
    except Exception as e:
        print("Failed to preprocess data,", str(e))
        return

    # iris custom response
    try:
        t3 = time.time()
        customResponse = []
        for line in data:
            res1 = get_response_from_iris(clientName, evaluatorEndpoint, headers, line)
            customResponse.append(res1)
        print("Time taken to get custom response from iris", ((time.time() - t3) / 60), "minute")
        key1 = 'output/' + dir_name + '/custom_response.json'
        generate_json_response(s3_client, s3_output_bucket, key1, customResponse, flowId)
    except Exception as e:
        print("Failed to generate custom response,", str(e))
        return

    # iris complete response
    if metric:
        completeResponse = True
    if completeResponse:
        evaluatorEndpoint = evaluatorEndpoint + "?tableSuffix=trial&isCompleteResponse=true"
        try:
            t4 = time.time()
            completeResp = []
            for line in data:
                res2 = get_response_from_iris(clientName, evaluatorEndpoint, headers, line)
                completeResp.append(res2)
            print("Time taken to get complete response from iris", ((time.time() - t4) / 60), "minute")
            key2 = 'output/' + dir_name + '/complete_response.json'
            generate_json_response(s3_client, s3_output_bucket, key2, completeResp, flowId)
        except Exception as e:
            print("Failed to generate complete response,", str(e))
            return

    # iris customField response
    if customFields:
        try:
            if completeResp:
                customFieldResponse = custom_attribute(completeResp, customFields)
            else:
                customFieldResponse = custom_attribute(customResponse, customFields)
            key3 = 'output/' + dir_name + '/custom_field_response.json'
            generate_json_response(s3_client, s3_output_bucket, key3, customFieldResponse, flowId)
            df = json_normalize(customResponse)
            # csv_buffer = StringIO()
            # df.to_csv(csv_buffer)
            key4 = 'output/' + dir_name + '/custom_field_response_table.csv'
            s3_client.put_object(Body=df.to_csv(), Bucket=s3_output_bucket, Key=key4)
        except Exception as e:
            print("Failed to generate custom field response,", str(e))
            return

    # generating CSV responses
    try:
        generate_csv_response(s3_output_bucket, s3_client, dir_name, clientName, metric, customResponse, completeResp,
                              dataLength)
    except Exception as e:
        print("Failed to generate csv response,", str(e))
        return

    print("Elapsed time", ((time.time() - total_time) / 60), "minute")

    return {
        'statusCode': 200,
        'body': 'Lambda function executed'
    }


if __name__ == "__main__":
    automation_testbed()