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

def getFileContent(s3_input_bucket, object_key, s3_client):
    print("Accessing data from s3...")
    file_content = s3_client.get_object(Bucket=s3_input_bucket, Key=object_key)["Body"].read().decode(
            "utf-8").splitlines(True)
    return file_content

def preProcess(clientName, applicationIds, lowerLimit, upperLimit, file_content):
    data = []
    if not applicationIds:
        for x in range(lowerLimit, upperLimit + 1):
            temp = json.loads(file_content[x])
            data.append(temp)
    else:
        print("Getting given applicationIds...")
        for line in file_content:
            temp = json.loads(line)

            if clientName == 'MeridianLink':
                try:
                    if int(temp['values']['input']['Application']['CLF']['VEHICLE_LOAN']['SYSTEM']['@loan_number']) in applicationIds:
                        data.append(temp)
                except:
                    if int(temp['values']['input']['Application']['CLF']['VEHICLE_LOAN']['SYSTEM'][0]['@loan_number']) in applicationIds:
                        data.append(temp)
            else:
                x = getAppID(temp)
                if x in applicationIds and x != 0:
                    data.append(temp)
    return data

def getResponseFromIris(clientName,evaluatorEndpoint,headers,line):
    try:
        if clientName == 'MeridianLink':
            payload = line
            try:
                print(payload['values']['input']['Application']['CLF']['VEHICLE_LOAN']['SYSTEM']['@loan_number'], end = ' ')
            except:
                print(payload['values']['input']['Application']['CLF']['VEHICLE_LOAN']['SYSTEM'][0]['@loan_number'], end = ' ')
        else:
            payload = line['sources']
            #payload = line['sources']['values']['input']
        payload = json.dumps(payload)
        response = requests.request("POST", evaluatorEndpoint, headers=headers, data=payload)
        respv = json.loads(response.text)
    except Exception as e:
        return {
            'statusCode':404,
            'body': str(e)
        }
    return respv

def generateCSVResponse(s3_output_bucket, s3_client, dirname, clientName, metric, customResponse, completeResp, dataLength):
    # csv files
    df = json_normalize(customResponse[:-1])
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    key = 'output/' + dirname + '/custom_response_table.csv'
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=s3_output_bucket, Key=key)

    if completeResp:
        df2 = json_normalize(completeResp)
        csv_buffer2 = StringIO()
        df2.to_csv(csv_buffer2)
        key = 'output/' + dirname + '/complete_response_table.csv'
        s3_client.put_object(Body=csv_buffer2.getvalue(), Bucket=s3_output_bucket, Key=key)

    # metric file generation
    if clientName == 'Numerica' and metric:
        metric_dict = pd.DataFrame(
            {'Decision': df2["Response.Decision"].value_counts(), 'App_Grade': df2["Response.App_Grade"].value_counts(),
             'CoApp_Grade': df2["Response.CoApp_Grade"].value_counts()})
    elif clientName == 'Numerica' and not metric:
        metric_dict = pd.DataFrame(
            {'Decision': df["Response.Decision"].value_counts(), 'App_Grade': df["Response.App_Grade"].value_counts(),
             'CoApp_Grade': df["Response.CoApp_Grade"].value_counts()})
    elif clientName == 'MeridianLink' and metric:
        metric_dict = getDict(completeResp,dataLength)
    else:
        metric_dict = pd.DataFrame()
    try:
        key = 'output/' + dirname + '/metric.csv'
        s3_client.put_object(Body=metric_dict.to_csv(), Bucket=s3_output_bucket, Key=key)
    except:
        print("Metric not generated.")

def generateJSONResponse(s3_client, S3_OUTPUT_BUCKET, key, response, flowId):
    response.append(flowId)
    s3_client.put_object(Body=json.dumps(response), Bucket=S3_OUTPUT_BUCKET, Key=key)

def customAttribute(responses, customFields):
    respv = []
    for response in responses:
        dict = {}
        df = json_normalize(response)
        for key in customFields:
            if isinstance(df[key].values[0], np.integer):
                dict[key] = df[key].values[0].item()
            else:
                dict[key] = df[key].values[0]
        jdump = json.dumps(dict)
        respv.append(json.loads(jdump))
    return respv

def getDict(completeResp, dataLength):
    applicants_0_scienaptic_score = []
    applicants_1_scienaptic_score = []
    scienaptic_score = []
    decision = []
    app_review_flags = []

    for x in completeResp[:-1]:
        if "Decision" in x["sources"]["values"]:
            if "Decision" in x["sources"]["values"]["Decision"]:
                decision.append(x["sources"]["values"]["Decision"]["Decision"])
            if "App_review_Flags" in x["sources"]["values"]["Decision"]:
                app_review_flags.append(x["sources"]["values"]["Decision"]["App_review_Flags"])
            if "scienaptic_score" in x["sources"]["values"]["Decision"]:
                i = x["sources"]["values"]["Decision"]["scienaptic_score"]
                if i is None:
                    scoreBucket = "None"
                    scienaptic_score.append(scoreBucket)
                else:
                    scoreBucket = getScoreBucket(i)
                    scienaptic_score.append(scoreBucket)

        if "LN_Bureau_and_Score" in x["sources"]["values"]:
            if "Applicants" in x["sources"]["values"]["LN_Bureau_and_Score"]:
                if "scienaptic_score" in x["sources"]["values"]["LN_Bureau_and_Score"]["Applicants"][0]["Scien_score"]:
                    j = x["sources"]["values"]["LN_Bureau_and_Score"]["Applicants"][0]["Scien_score"][
                        "scienaptic_score"]
                    if j is None:
                        app0 = "None"
                        applicants_0_scienaptic_score.append(app0)
                    else:
                        app0 = getScoreBucket(j)
                        applicants_0_scienaptic_score.append(app0)

                if "scienaptic_score" in x["sources"]["values"]["LN_Bureau_and_Score"]["Applicants"][1]["Scien_score"]:
                    k = x["sources"]["values"]["LN_Bureau_and_Score"]["Applicants"][1]["Scien_score"][
                        "scienaptic_score"]
                    if k is None:
                        app1 = "None"
                        applicants_1_scienaptic_score.append(app1)
                    else:
                        app1 = getScoreBucket(k)
                        applicants_1_scienaptic_score.append(app1)

    if not applicants_0_scienaptic_score:
        applicants_0_scienaptic_score.append("None")
    if not applicants_1_scienaptic_score:
        applicants_1_scienaptic_score.append("None")
    if not scienaptic_score:
        scienaptic_score.append("None")
    if not decision:
        decision.append("None")
    if not app_review_flags:
        app_review_flags.append("None")

    pd_df1 = pd.DataFrame(applicants_0_scienaptic_score)[0].value_counts()
    pd_df2 = pd.DataFrame(applicants_1_scienaptic_score)[0].value_counts()
    pd_df3 = pd.DataFrame(scienaptic_score)[0].value_counts()
    pd_df4 = pd.DataFrame(decision)[0].value_counts()

    flatList = [item for sublist in app_review_flags for item in sublist if
                item not in ["Scienaptic Recommendation: Approve", "Scienaptic Recommendation: Decline",
                             "Scienaptic Recommendation: Review"]]
    pd_df5 = pd.DataFrame(flatList)[0].value_counts()
    percentage = (pd_df5.values / dataLength) * 100

    # metric_dict = {"applicants_0_scienaptic_score": df1, "applicants_1_scienaptic_score": df2, "scienaptic_score": df3, "Decision": df4, "app_review_flags": df5}

    final1 = pd.DataFrame({'Range': pd_df1.index, 'No. of applicants': pd_df1.values})
    final2 = pd.DataFrame({'Range': pd_df2.index, 'No. of applicants': pd_df2.values})
    final3 = pd.DataFrame({'Range': pd_df3.index, 'No. of applicants': pd_df3.values})
    final4 = pd.DataFrame({'Decision': pd_df4.index, 'No. of applicants': pd_df4.values})
    final5 = pd.DataFrame({'Rules': pd_df5.index, 'No. of applicants': pd_df5.values, '% Apps': percentage})
    space = pd.DataFrame({"": ['', '', '', '', ]}, index=None, columns=None)

    a1 = ['Applicants_0_Scienaptic_Score', '']
    b1 = ['Range', 'Number of Applicants']
    a2 = ['Applicants_1_Scienaptic score', '']
    b2 = ['Range', 'Number of Applicants']
    a3 = ['Scienaptic_Score', '']
    b3 = ['Range', 'Number of Applicants']
    a4 = ['Decision', '']
    b4 = ['Decision', 'Number of Applicants']
    a5 = ['Rule Combinations Hit', '', '']
    b5 = ['Rules', '# Apps', '% Apps']
    a6 = ['']
    b6 = ['']

    final1.columns = pd.MultiIndex.from_arrays([a1, b1])
    final2.columns = pd.MultiIndex.from_arrays([a2, b2])
    final3.columns = pd.MultiIndex.from_arrays([a3, b3])
    final4.columns = pd.MultiIndex.from_arrays([a4, b4])
    final5.columns = pd.MultiIndex.from_arrays([a5, b5])
    space.columns = pd.MultiIndex.from_arrays([a6, b6])
    metric_dict = pd.concat([final1, space, final2, space, final3, space, final4, space, final5], axis=1)
    return metric_dict

def getAppID(j):
    flat = flatten_json(j)
    for key,value in flat.items():
        lst = key.split(".")[-1]
        if lst == "App_ID" or lst == "ApplicationId":
            return value
        else:
            continue
    return 0

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '.')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

def getScoreBucket(score):
    if score < 600:
        return "<600"
    elif 600 <= score < 650:
        return "600 - 650"
    elif 650 <= score < 700:
        return "650 - 700"
    elif 700 <= score < 750:
        return "700 - 750"
    elif 750 <= score < 800:
        return "750 - 800"
    elif score >= 800:
        return ">"

def centralizeAutomation():
    print("Inside lambda handler.")
    s3_input_bucket = 'testbed-input-data'
    s3_client = boto3.client('s3')
    input_key = "input.json"
    total_time = time.time()
    try:
        input_event = s3_client.get_object(Bucket=s3_input_bucket, Key=input_key)['Body'].read().decode('utf-8')
    except:
        traceback.print_exc()
        return
    event = json.loads(input_event)
    print("Input reading finished.")
    s3_output_bucket = event['s3_output_bucket']
    input_file = event['input_file']
    los = event['combinations']['los']
    buereau = event['combinations']['buereau']
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
    if los and buereau:
        dirname = los + '&' + buereau
    elif los:
        dirname = los
    elif buereau:
        dirname = buereau
    else:
        return {
            'statusCode': 404,
            'body': 'Please enter los and/or buereau'
        }

    if not authToken.startswith('Bearer'):
        authToken = 'Bearer ' + authToken
    headers = {
        'Content-Type': 'application/json',
        'Authorization': authToken
    }
    # input object key path
    object_key = dirname + '/' + input_file

    # getting file contents
    try:
        t1 = time.time()
        file_content = getFileContent(s3_input_bucket, object_key, s3_client)
        print("Time taken to get and split data from s3", ((time.time() - t1) / 60), "minute")
    except:
        traceback.print_exc()
        return

    if lowerLimit < 0 or lowerLimit > len(file_content) or upperLimit < 0 or upperLimit > len(file_content):
        print("Index out of range. Please enter correct upper and lower limit.")
        return
    # preprocessing data
    try:
        t2 = time.time()
        data = preProcess(clientName, applicationIds, lowerLimit, upperLimit, file_content)
        dataLength = len(data)
        print("Time taken to load data from file_content", ((time.time() - t2) / 60), "minute")
        print("Number of applicant data loaded from json:", dataLength)
    except:
        traceback.print_exc()
        return

    # iris custom response
    try:
        t3 = time.time()
        customResponse = []
        for line in  data:
            res1 = getResponseFromIris(clientName, evaluatorEndpoint, headers, line)
            customResponse.append(res1)
        print("Time taken to get custom response from iris",((time.time() - t3)/60),"minute")
        key1 = 'output/' + dirname + '/custom_response.json'
        generateJSONResponse(s3_client, s3_output_bucket, key1, customResponse, flowId)
    except:
        traceback.print_exc()
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
                res2 = getResponseFromIris(clientName, evaluatorEndpoint, headers, line)
                completeResp.append(res2)
            print("Time taken to get complete response from iris", ((time.time() - t4)/60),"minute")
            key2 = 'output/' + dirname + '/complete_response.json'
            generateJSONResponse(s3_client, s3_output_bucket, key2, completeResp, flowId)
        except:
            traceback.print_exc()
            return

    # iris customField response
    if customFields:
        try:
            if completeResp:
                customFieldResponse = customAttribute(completeResp, customFields)
            else:
                customFieldResponse = customAttribute(customResponse, customFields)
            key3 = 'output/' + dirname + '/custom_field_response.json'
            generateJSONResponse(s3_client, s3_output_bucket, key3, customFieldResponse, flowId)
            df = json_normalize(customResponse)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            key4 = 'output/' + dirname + '/custom_field_response_table.csv'
            s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=s3_output_bucket, Key=key4)
        except:
            traceback.print_exc()
            return

    # generating CSV responses
    try:
        generateCSVResponse(s3_output_bucket, s3_client, dirname, clientName, metric, customResponse, completeResp, dataLength)
    except:
        traceback.print_exc()
        return

    print("Total time taken", ((time.time() - total_time) / 60), "minute")
    return {
        'statusCode': 200,
        'body': 'Lambda function executed'
    }

if __name__ == "__main__":
    centralizeAutomation()