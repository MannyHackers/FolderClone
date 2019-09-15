from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import base64, json, glob, sys, argparse, time, os.path, pickle, requests, random

SCOPES = ["https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/cloud-platform","https://www.googleapis.com/auth/iam"]
proj_id = json.loads(open('credentials.json','r').read())['installed']['project_id']
failed_create = []
unique_ids = []
keys = []

def _create_accounts(service,todo,prefix,project):
    batch = service.new_batch_http_request(callback=_get_unique_id)
    for o in todo:
        batch.add(service.projects().serviceAccounts().create(name="projects/" + project, body={ "accountId": prefix + str(o), "serviceAccount": { "displayName": prefix + str(o) }}))
    batch.execute()

def _get_unique_id(id,resp,exception):
    global unique_ids
    global failed_create

    if exception is not None:
        err_msg = json.loads(exception.content.decode('utf-8'))['error']['message']
        if err_msg == 'Maximum number of service accounts on project reached.':
            pass
        else:
            failed_create.append(resp['uniqueId'])
    else:
        unique_ids.append(resp['uniqueId'])
        
def _get_keys(service,project,ids):
    batch = service.new_batch_http_request(callback=_get_key)
    for o in ids:
        batch.add(service.projects().serviceAccounts().keys().create(name="projects/%s/serviceAccounts/%s" % (project,o), body={ "privateKeyType": "TYPE_GOOGLE_CREDENTIALS_FILE", "keyAlgorithm": "KEY_ALG_RSA_2048" }))
    batch.execute()

def _get_key(id,resp,exception):
    global keys
    if exception is not None:
        print(json.loads(exception.content.decode('utf-8'))['error'])
    else:
        keys.append(resp['privateKeyData'])

def _generate_id():
    chars = '-abcdefghijklmnopqrstuvwxyz1234567890'
    return 'saf-' + ''.join(random.choice(chars) for _ in range(25)) + random.choice(chars[1:])

def _get_projects(service):
    return service.projects().list().execute()['projects']
    
def _def_batch_resp(id,resp,exception):
    if exception is not None:
        print(str(exception))

creds = None
if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

cloud = build("cloudresourcemanager", "v1", credentials=creds)
iam = build("iam", "v1", credentials=creds)
services = build("serviceusage","v1",credentials=creds)

parse = argparse.ArgumentParser(description='A tool to create Google service accounts.')
parse.add_argument('--path','-p',default='accounts',help='Specify an alternate directory to output the credential files.')
parse.add_argument('--fill-to',help='Max amount of projects to create.')
parse.add_argument('--email-prefix',default='folderclone',help='Prefix to use for service account emails.')

args = parse.parse_args()

projs = None
while projs == None:
    try:
        projs = _get_projects(cloud)
    except HttpError as e:
        if json.loads(e.content.decode('utf-8'))['error']['status'] == 'PERMISSION_DENIED':
            try:
                services.services().enable(name='projects/%s/services/cloudresourcemanager.googleapis.com' % proj_id).execute()
            except HttpError as e:
                print(e._get_reason())
                input('Press Enter to retry.')

print('Using %s' % proj_id)
print('Found %s projects.' % len(projs))

fill = int(input('Fill to? '))

batch = cloud.new_batch_http_request(callback=_def_batch_resp)
for i in range(fill - len(projs)):
    new_proj = _generate_id()
    projs.append({'projectId':new_proj})
    batch.add(cloud.projects().create(body={'project_id':new_proj}))
batch.execute()

print('Sleeping...')
time.sleep((fill - len(projs)) * 3)

batch = services.new_batch_http_request(callback=_def_batch_resp)
for i in projs:
    batch.add(services.services().enable(name='projects/%s/services/iam.googleapis.com' % i['projectId']))
    batch.add(services.services().enable(name='projects/%s/services/drive.googleapis.com' % i['projectId']))
batch.execute()

sas = 0
for i in projs:
    unique_ids = []
    keys = []
    _create_accounts(iam,list(range(100)),args.email_prefix,i['projectId'])
    retry_count = 0
    while len(failed_create) > 0:
        retry_count += 1
        retry = failed_create
        failed_create = []
        _create_accounts(iam,retry,args.email_prefix + "-r-" + str(retry_count) + "-",i['projectId'])
    for i in iam.projects().serviceAccounts().list(name='projects/' + i['projectId'],pageSize=100).execute()['accounts']:
        unique_ids.append(i['uniqueId'])
    _get_keys(iam,i['projectId'],unique_ids)
    for o in keys:
        sas += 1
        with open('%s/SA%d.json' % (args.path,sas),'w+') as f:
            f.write(base64.b64decode(o).decode('utf-8'))