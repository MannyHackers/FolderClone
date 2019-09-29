from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import base64, json, progress.bar, glob, sys, argparse, time, os.path, pickle, requests, random

SCOPES = ["https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/cloud-platform","https://www.googleapis.com/auth/iam"]
project_create_ops = []

def _create_accounts(service,project,count):
    batch = service.new_batch_http_request(callback=_def_batch_resp)
    for i in range(count):
        aid = _generate_id('mfc-')
        batch.add(service.projects().serviceAccounts().create(name="projects/" + project, body={ "accountId": aid, "serviceAccount": { "displayName": aid }}))
    batch.execute()

def _create_remaining_accounts(iam,project):
    print('Creating accounts in %s' % project)
    sa_count = len(_list_sas(iam,project))
    while sa_count < 100:
        _create_accounts(iam,project,100 - sa_count)
        sa_count = len(_list_sas(iam,project))

def _generate_id(prefix='saf-'):
    chars = '-abcdefghijklmnopqrstuvwxyz1234567890'
    return prefix + ''.join(random.choice(chars) for _ in range(25)) + random.choice(chars[1:])

def _get_projects(service):
    return service.projects().list().execute()['projects']
    
def _pc_resp(id,resp,exception):
    global project_create_ops
    if exception is not None:
        print(str(exception))
    else:
        for i in resp.values():
            project_create_ops.append(i)

def _def_batch_resp(id,resp,exception):
    if exception is not None:
        print(str(exception))

def _create_projects(cloud,count):
    batch = cloud.new_batch_http_request(callback=_pc_resp)
    for i in range(count):
        new_proj = _generate_id()
        batch.add(cloud.projects().create(body={'project_id':new_proj}))
    batch.execute()

    for i in project_create_ops:
        while True:
            resp = cloud.operations().get(name=i).execute()
            if 'done' in resp and resp['done']:
                break
            time.sleep(3)
    return _get_projects(cloud)

def _enable_services(service,projects,ste):
    batch = service.new_batch_http_request(callback=_def_batch_resp)
    for i in projects:
        for j in ste:
            batch.add(service.services().enable(name='projects/%s/services/%s' % (i,j)))
    batch.execute()

def _list_sas(iam,project):
    resp = iam.projects().serviceAccounts().list(name='projects/' + project,pageSize=100).execute()
    if 'accounts' in resp:
        return resp['accounts']
    return []
    
def _create_sa_keys(iam,projects,path):
    for i in projects:
        total_sas = _list_sas(iam,i)
        pbar = progress.bar.Bar('Downloading keys from %s' % i, max=len(total_sas))
        for j in total_sas:
            sakey = iam.projects().serviceAccounts().keys().create(
                name='projects/%s/serviceAccounts/%s' % (i,j['uniqueId']),
                body={
                    'privateKeyType':'TYPE_GOOGLE_CREDENTIALS_FILE',
                    'keyAlgorithm':'KEY_ALG_RSA_2048'
                }
            ).execute()
            with open('%s/%s.json' % (path,j['displayName']),'w+') as f:
                f.write(base64.b64decode(sakey['privateKeyData']).decode('utf-8'))
            pbar.next()
        pbar.finish()

def serviceaccountfactory(path=None,credentials='credentials.json',download_keys=None,create_sas=None,token='token.pickle',enable_services=None,list_projects=False,list_sas=None,create_projects=None,services=None):
    proj_id = json.loads(open(credentials,'r').read())['installed']['project_id']
    creds = None
    if os.path.exists(token):
        with open(token, 'rb') as t:
            creds = pickle.load(t)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token, 'wb') as t:
            pickle.dump(creds, t)

    cloud = build("cloudresourcemanager", "v1", credentials=creds)
    iam = build("iam", "v1", credentials=creds)
    serviceusage = build("serviceusage","v1",credentials=creds)

    projs = None
    while projs == None:
        try:
            projs = _get_projects(cloud)
        except HttpError as e:
            if json.loads(e.content.decode('utf-8'))['error']['status'] == 'PERMISSION_DENIED':
                try:
                    serviceusage.services().enable(name='projects/%s/services/cloudresourcemanager.googleapis.com' % proj_id).execute()
                except HttpError as e:
                    print(e._get_reason())
                    input('Press Enter to retry.')
    if list_projects:
        return _get_projects(cloud)
    elif list_sas:
        return _list_sas(iam,list_sas)
    elif create_projects:
        if create_projects > 0:
            current_count = len(_get_projects(cloud))
            if current_count < create_projects:
                print('Creating %d projects' % (create_projects - current_count))
                _create_projects(cloud, create_projects - current_count)
                print('Done.')
            else:
                print('%d projects or more already exist!' % current_count)
        else:
            print('Please specify a number larger than 0.')
    elif enable_services:
        if enable_services == '*':
            ste = [i['projectId'] for i in _get_projects(cloud)]
        else:
            ste = []
            ste.append(enable_services)
        for i in services:
            i = i + '.googleapis.com'
        print('Enabling services')
        _enable_services(serviceusage,ste,services)
        print('Done')
    elif create_sas:
        if create_sas == '*':
            for i in [i['projectId'] for i in _get_projects(cloud)]:
                _create_remaining_accounts(iam,i)
        else:
            _create_remaining_accounts(iam,create_sas)
        print('Done.')
    elif download_keys:
        if download_keys == '*':
            std = [i['projectId'] for i in _get_projects(cloud)]
        else:
            std = []
            std.append(download_keys)
        _create_sa_keys(iam,std,path)
        print('Done')
    else:
        print('Unknown.')


if __name__ == '__main__':
    parse = argparse.ArgumentParser(description='A tool to create Google service accounts.')
    parse.add_argument('--path','-p',default='accounts',help='Specify an alternate directory to output the credential files.')
    parse.add_argument('--token',default='token.pickle',help='Specify the pickle token file path.')
    parse.add_argument('--credentials',default='credentials.json',help='Specify the credentials file path.')
    parse.add_argument('--list-projects',default=False,action='store_true',help='List projects managable by the user.')
    parse.add_argument('--enable-services',default=None,help='Enables services on the project. Default: IAM and Drive')
    parse.add_argument('--services',nargs='+',default=['iam','drive'],help='Specify a different set of services to enable. Overrides the default.')
    parse.add_argument('--create-projects',type=int,default=None,help='Creates up to N projects. Takes into account existing projects.')
    parse.add_argument('--list-sas',default=False,help='List service accounts in a project.')
    parse.add_argument('--create-sas',default=None,help='Create service accounts in a project.')
    parse.add_argument('--download-keys',default=None,help='Download keys for all the service accounts in a project.')
    args = parse.parse_args()
    resp = serviceaccountfactory(
        path=args.path,
        token=args.token,
        credentials=args.credentials,
        list_projects=args.list_projects,
        list_sas=args.list_sas,
        create_projects=args.create_projects,
        create_sas=args.create_sas,
        enable_services=args.enable_services,
        services=args.services,
        download_keys=args.download_keys
    )
    if resp is not None:
        if args.list_projects:
            print('Projects:')
            for i in resp:
                print('  ' + i['projectId'])
        elif args.list_sas:
            print('Service accounts in %s:' % args.list_sas)
            for i in resp:
                print('  %s (%s)' % (i['email'],i['uniqueId']))
