import os
import requests
import json
import time
from dotenv import load_dotenv
import sys
import pandas as pd
import io


load_dotenv()
# set global vars
username = os.getenv('user')
password = os.getenv('password')
workspaceId = os.getenv('workspaceId')
modelId = os.getenv('modelId')

domain = 'https://api.anaplan.com/2/0'


def error_dump(function_name, response):
    print(f'Error in {function_name} ............')
    print(f'Request failed with status code: {response.status_code}')
    print(f'URL: {response.url} ')
    print('request headers: ', response.request.headers)
    print('request body   : ', response.request.body)
    print('response       : ', response.json())



def get_auth_token(username=username, password=password):
    url = 'https://auth.anaplan.com/token/authenticate'
    response = requests.request("POST", url, auth=(username, password))

    if response.json()['status'] == 'SUCCESS':
        return response.json()['tokenInfo']['tokenValue']
    else:
        print('authentication failed')
        sys.exit('Terminating the script as authentication failed')


def list_user_workspaces():
    path = '/workspaces?tenantDetails=true'
    url = domain + path

    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()['workspaces']
    else:
        print(f'Request failed with status code {response.status_code}')
        print(response.json())
        return False

def retrieve_workspace_information(workspaceId):
    path = f'/workspaces/{workspaceId}?tenantDetails=true'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Request failed with status code {response.status_code}')
        print(response.json())
        return False

def list_models():
    """Retrieves information about all models in the user's default tenant."""
    path = f'/models'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()['models']
    else:
        print(f'Request failed with status code {response.status_code}')
        print(response.json())
        return False


def retrieve_models_in_workspace(workspaceId):
    """Retrieves information about all models in the specified workspace."""

    path = f'/workspaces/{workspaceId}/models'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:

        print(f'Request failed with status code {response.status_code}')
        print(response.json())
        return False


def retrieve_a_specific_model(modelId=modelId):
    """Retrieves information about a specific model in the user's default tenant, if the user has access to the model."""

    path = f'/models/{modelId}?modelDetails=True'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Request failed with status code {response.status_code}')
        print(response.json())
        return False


def check_model_status(workspaceId=workspaceId, modelId=modelId):
    """
    Certain actions such as imports, exports, or writeback actions require exclusive access to a
    model during their execution. These actions lock the model. Any attempt to read information
    via the API is blocked as they require an exclusive transaction to run.
    This endpoint provides a status for the model.

    response['requestStatus']['currentStep'] = 'Processing ...' >>> means there's currently an action running
    response['requestStatus']['currentStep'] = 'Open'           >>> means nothing is currently going on

    """
    path = f'/workspaces/{workspaceId}/models/{modelId}/status'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Request failed with status code {response.status_code}')
        print(response.json())
        return False


# Upload File Actions
def get_the_id_and_information_for_the_model_file_to_upload(workspaceId, modelId):

    path = f'/workspaces/{workspaceId}/models/{modelId}/files'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False


def get_model_id(modelName):
    """Get Model Id from Name"""
    response = list_models()
    for model in response:
        if model['name'] == modelName:
            return model['id']
    print("Couldn't find model")
    return False


def get_workspace_id(workspaceName):
    """Get Workspace Id from Name"""
    response = list_user_workspaces()
    for workspace in response:
        if workspace['name'] == workspaceName:
            return workspace['id']

    print("Couldn't find Workspace")
    return False



def upload_file_as_single_chunk(fileId, filename):
    """For files < 50mb"""
    path = f'/workspaces/{workspaceId}/models/{modelId}/files/{fileId}'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/octet-stream'
    }

    file = open(filename, 'rb')
    response = requests.request('PUT', url=url, data=file, headers=headers)

    if response.status_code == 204:
        return f'Uploaded {filename} with status code {response.status_code}'
    else:
        error_dump('upload_file_as_single_chunk', response)
        return False


def upload_content_as_single_chunk(fileId, content):
    """For files < 50mb"""
    path = f'/workspaces/{workspaceId}/models/{modelId}/files/{fileId}'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/octet-stream'
    }

    #file = open(filename, 'rb')
    response = requests.request('PUT', url=url, data=content, headers=headers)

    if response.status_code == 204:
        return f'Uploaded {fileId} with status code {response.status_code}'
    else:
        error_dump('upload_file_as_single_chunk', response)
        return False


def list_imports():
    path = f'/workspaces/{workspaceId}/models/{modelId}/imports'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()['imports']
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False


def get_import_id(importIdName):
    response = list_imports()
    for item in response:
        if item['name'] == importIdName:
            return item['id']
    print("Couldn't find file")
    print(response.json())
    return False

def list_files():
    path = f'/workspaces/{workspaceId}/models/{modelId}/files'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()['files']
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False


def get_file_id(fileName):
    response = list_files()
    for item in response:
        if item['name'] == fileName:
            return item['id']
    print("Cant't find file")
    return False


def start_export(exportId):
    """Run Action Returns taskId"""

    path = f'/workspaces/{workspaceId}/models/{modelId}/exports/{exportId}/tasks'

    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    payload = json.dumps({'localeName': 'en_GB'})

    response = requests.request('POST', url=url, headers=headers, data=payload)

    if response.status_code == 200:
        return response.json()
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False

def get_export_task_status(exportId, taskId):
    """Get the status of an action that has run"""
    path = f'/workspaces/{workspaceId}/models/{modelId}/exports/{exportId}/tasks/{taskId}'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        print(response.json())
        return response.json()['task']['taskState']

    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return response.json()


def start_action(actionId):
    """Run Action Returns taskId"""

    path = f'/workspaces/{workspaceId}/models/{modelId}/actions/{actionId}/tasks'

    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    payload = json.dumps({'localeName': 'en_GB'})

    response = requests.request('POST', url=url, headers=headers, data=payload)

    if response.status_code == 200:
        return response.json()
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False


def get_action_task_status(actionId, taskId):
    """Get the status of an action that has run"""
    path = f'/workspaces/{workspaceId}/models/{modelId}/actions/{actionId}/tasks/{taskId}'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return response.json()

def run_action(actionId):
    """Runs the whole action, waits for 5 seconds and returns the task status"""
    response = start_action(actionId)
    time.sleep(5)

    taskId = response['task']['taskId']
    response = get_action_task_status(actionId, taskId)

    return response


def start_import(importId):
    """Starts import and returns task"""

    path = f'/workspaces/{workspaceId}/models/{modelId}/imports/{importId}/tasks/'

    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    payload = json.dumps({'localeName': 'en_GB'})

    response = requests.request('POST', url=url, headers=headers, data=payload)

    if response.status_code == 200:
        return response.json()
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False

def get_import_task_status(importId, taskId):
    path = f'/workspaces/{workspaceId}/models/{modelId}/imports/{importId}/tasks/{taskId}'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()['task']['taskState']
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False

def run_import(importId):
    """Run import id, wait 5 seconds and return task status"""

    response = start_import(importId)
    time.sleep(5)
    taskId = response['task']['taskId']
    response = get_import_task_status(importId, taskId)

    return response


def list_delete_actions():
    path = f'/workspaces/{workspaceId}/models/{modelId}/actions'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()['actions']
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False


def get_delete_action_id(deleteActionName):
    response = list_delete_actions()
    for item in response:
        if item['name'] == deleteActionName:
            return item['id']

    print("Couldn't find delete action")
    return False

def list_processes():

    """List processes"""
    path = f'/workspaces/{workspaceId}/models/{modelId}/processes'
    url = domain + path
    #anaplan_auth_token = get_auth_token()

    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()['processes']
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False

def get_process_id(processName):
    response = list_processes()
    for item in response:
        if item['name'] == processName:
            return item['id']
    print("Cant't find process")
    return False



def get_process_metadata(processId):
    """Get metadata for an export"""
    path = f'/models/{modelId}/processes/{processId}'
    url = domain + path
    #anaplan_auth_token = get_auth_token()

    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False

def start_process(processId):
    """Starts process and returns task"""

    path = f'/workspaces/{workspaceId}/models/{modelId}/processes/{processId}/tasks'

    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    payload = json.dumps({'localeName': 'en_GB'})

    response = requests.request('POST', url=url, headers=headers, data=payload)

    if response.status_code == 200:
        return response.json()
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        #print(response.json())
        return False


def get_process_task_status(processId, taskId):
    path = f'/workspaces/{workspaceId}/models/{modelId}/processes/{processId}/tasks/{taskId}'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        error_dump('get_process_task_status()', response)
        return False


def run_process(processId):
    """Run import id, wait 5 seconds and return task status"""

    response = start_process(processId)
    time.sleep(5)
    taskId = response['task']['taskId']
    response = get_process_task_status(processId, taskId)

    while response['task']['taskState'] != 'COMPLETE':
        print(response['task']['taskState'])
        time.sleep(3)
        response = get_process_task_status(processId, taskId)

    print('Task COMPLETE')

    return response


def list_exports():

    """List exports"""
    path = f'/workspaces/{workspaceId}/models/{modelId}/exports'
    url = domain + path
    #anaplan_auth_token = get_auth_token()

    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()['exports']
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False

def get_export_metadata(exportId):
    """Get metadata for an export"""
    path = f'/workspaces/{workspaceId}/models/{modelId}/exports/{exportId}'
    url = domain + path
    #anaplan_auth_token = get_auth_token()

    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False

def list_export_files():
    """Get metadata for an export"""
    path = f'/workspaces/{workspaceId}/models/{modelId}/files'
    url = domain + path


    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/json'
    }

    response = requests.request('GET', url=url, headers=headers)
    if response.status_code == 200:
        return response.json()['files']
    else:
        print(f'URL: {url} \nRequest failed with status code {response.status_code}')
        print(response.json())
        return False


def get_export_id(exportName):
    """Get the ID of an export name"""
    response = list_exports()
    for item in response:
        if item['name'] == exportName:
            return item['id']

    print("Couldn't find export action")
    return False



def run_export_till_complete(exportId, max_tries=20):
    """Runs an export till task status is complete"""
    r3 = start_export(exportId)
    taskId = r3['task']['taskId']
    time.sleep(1)
    status = get_export_task_status(exportId, taskId)
    count = 0
    while status != 'COMPLETE' or count == max_tries:
        time.sleep(1)
        count += 1
        print(f'Check Status for task {taskId}: {count}')
        status = get_export_task_status(exportId, taskId)

    if count == max_tries:
        return 'Failed to complete within max_tries'
    else:
        return status


def run_import_till_complete(importId, max_tries=20):
    """Runs an export till task status is complete"""
    r3 = start_import(importId)
    taskId = r3['task']['taskId']
    time.sleep(1)
    status = get_import_task_status(importId, taskId)

    count = 0
    while status != 'COMPLETE' or count == max_tries:
        time.sleep(1)
        count += 1
        print(f'Check Status for task {taskId}: {count}')
        status = get_import_task_status(importId, taskId)

    if count == max_tries:
        return 'Failed to complete within max_tries'
    else:
        return status



def download_export_file(fileId, filename):

    path = f'/workspaces/{workspaceId}/models/{modelId}/files/{fileId}/chunks/0'
    url = domain + path

    headers = {
        'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
        'Content-Type': 'application/json'
    }

    response = requests.request("GET", url, headers=headers)

    with open(filename, 'wb') as f:
        f.write(response.content)

    return 'OK'


def download_export_as_df(exportId, run_export=True):
    """Downloads an export file after running the export action"""
    if run_export:
        resp = run_export_till_complete(exportId)
        if resp == 'Failed to complete within max_tries':
            print('Failed to run export')
            return 'Failed to run export'

    url = f'https://api.anaplan.com/2/0/workspaces/{workspaceId}/models/{modelId}/files/{exportId}/chunks/0'

    headers = {
        'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
        'Accept': 'application/octet-stream'
    }

    response = requests.request("GET", url, headers=headers)
    r = response.content

    df = pd.read_csv(io.BytesIO(r))

    return df



def run_export_and_download(exportId, fileId, sleep=2):
    """Runs an export, waits for 2 seconds and downloads the file"""
    response = start_export(exportId)

    time.sleep(sleep)

    #taskId = response['task']['taskId']
    # response = get_export_task_status(exportId, taskId)
    # print(response)

    response = download_export_file(fileId)

    return response

def upload_df_as_single_chunk(fileId, df):
    """For files < 50mb"""
    path = f'/workspaces/{workspaceId}/models/{modelId}/files/{fileId}'
    url = domain + path
    headers = {
    'Authorization': f'AnaplanAuthToken {anaplan_auth_token}',
    'Content-Type': 'application/octet-stream'
    }

    content = df.to_csv(index=False)

    #file = open(filename, 'rb')
    response = requests.request('PUT', url=url, data=content, headers=headers)

    if response.status_code == 204:
        return f'Uploaded {fileId} with status code {response.status_code}'
    else:
        error_dump('upload_file_as_single_chunk', response)
        return False


def upload_df_and_run_import(fileId, importId, df):
    """Uploads a DataFrame into an Anaplan file and runs the import action"""
    upload_df_as_single_chunk(fileId, df)
    resp = run_import_till_complete(importId)
    return resp


anaplan_auth_token = get_auth_token()


if __name__ == '__main__':
    pass


