from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from os import mkdir,path
import json

def chunk(list_to_chunk,size):
    return [ list_to_chunk[i:i + size] for i in range(0, len(list_to_chunk), size) ]

def get_creds(credentials,token,scopes=['https://www.googleapis.com/auth/drive']):
    creds = None

    if path.exists(token):
        with open(token,'r') as t:
            creds = json_to_cred(t)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials,scopes)
            creds = flow.run_local_server(port=0)
        with open(token,'w') as t:
            json.dump(cred_to_json(creds),t,indent=2)

    return creds

def cred_to_json(cred_to_pass):
    cred_json = {
        'token': cred_to_pass.token,
        'refresh_token': cred_to_pass.refresh_token,
        'id_token': cred_to_pass.id_token,
        'token_uri': cred_to_pass.token_uri,
        'client_id': cred_to_pass.client_id,
        'client_secret': cred_to_pass.client_secret,
    }
    return cred_json

def json_to_cred(json_to_pass):
    cred_json = json.load(json_to_pass)
    creds = Credentials(
        cred_json['token'],
        refresh_token=cred_json['refresh_token'],
        id_token=cred_json['id_token'],
        token_uri=cred_json['token_uri'],
        client_id=cred_json['client_id'],
        client_secret=cred_json['client_secret']
    )
    return creds
