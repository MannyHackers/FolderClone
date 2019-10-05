from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from argparse import ArgumentParser
from folderclone._helpers import *
from base64 import b64decode
from os.path import exists
from random import choice
from json import loads
from time import sleep
from uuid import uuid4
from os import rename
from glob import glob
from sys import exit

class multimanager():
    credentials = 'credentials.json'
    token = 'token.json'
    drive_service = None
    cloud_service = None
    usage_service = None
    iam_service = None
    proj_id = None
    max_projects = 12
    creds = None
    project_create_ops = []
    current_key_dump = []
    successful = []
    to_be_removed = []
    sleep_time = 30
    def _build_service(self,service,v):
        """Builds a new service.

        Given api service and version v, builds and returns a new service.

        Parameters:
            service (str): The API service name.
            v (str): The API version.

        Returns:
            Resource: The service used to interact with the API.
        """
        SCOPES = ["https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/cloud-platform","https://www.googleapis.com/auth/iam"]
        self.creds = get_creds(self.credentials,self.token,scopes=SCOPES)
        return build(service,v,credentials=self.creds)

    def __init__(self,**options):
        """Creates a new multimanager object.

        Optional Parameters:
            credentials (str): The path for the credentials file. (default credentials.json)
            token (str): The path for the token file. (default token.json)
            sleep_time (int): The amound of seconds to sleep between errors. (default 30)
            max_projects (int): Max amount of project allowed. (default 12)
            usage_service: A Service Usage service
            iam_service: An IAM service.
            drive_service: A Drive service.
            cloud_service: A Cloud Resource Manager service.
        """
        if options.get('credentials') is not None:
            self.credentials = str(options['credentials'])
        if options.get('token') is not None:
            self.token = str(options['token'])
        if options.get('usage_service') is not None:
            self.usage_service = options['usage_service']
        else:
            self.usage_service = self._build_service('serviceusage','v1')
        if options.get('iam_service') is not None:
            self.iam_service = options['iam_service']
        else:
            self.iam_service = self._build_service('iam','v1')
        if options.get('drive_service') is not None:
            self.drive_service = options['drive_service']
        else:
            self.drive_service = self._build_service('drive','v3')
        if options.get('cloud_service') is not None:
            self.cloud_service = options['cloud_service']
        else:
            self.cloud_service = self._build_service('cloudresourcemanager','v1')
        if options.get('sleep_time') is not None:
            self.sleep_time = int(options['sleep_time'])
        if options.get('max_projects') is not None:
            self.max_projects = int(options['max_projects'])

        projs = None
        retries = 0
        self.proj_id = loads(open(self.credentials,'r').read())['installed']['project_id']
        while type(projs) is not list:
            if retries > 5:
                raise RuntimeError('Could not use Cloud Resource Manager API.')
            retries += 1
            try:
                projs = self.list_projects()
            except HttpError as e:
                if loads(e.content.decode('utf-8'))['error']['message'].startswith('Cloud Resource Manager API has not been used in project'):
                    try:
                        self.usage_service.services().enable(name='projects/%s/services/cloudresourcemanager.googleapis.com' % self.proj_id).execute()
                    except Exception as e:
                        print(e._get_reason())
                        input('Press Enter to retry.')
                else:
                    raise e

    def _drive_execute(self,to_execute):
        try:
            return to_execute.execute()
        except HttpError as e:
            if loads(e.content.decode('utf-8'))['error']['message'].startswith('Access Not Configured. Drive API has not been used in project'):
                self.enable_services([self.proj_id],['drive'])
                raise RuntimeError('Drive API has been enabled. Please wait a few minutes before trying again.')

    def create_shared_drive(self,name):
        """Creates a new Shared Drive.

        Parameters:
            name: string, the name for the Shared Drive
        Returns:
            dict, the new Shared Drive name and id.
        """
        return self._drive_execute(self.drive_service.drives().create(body={'name': name},requestId=str(uuid4()),fields='id,name'))

    def list_shared_drives(self):
        return self._drive_execute(self.drive_service.drives().list(fields='drives(id,name)'))['drives']

    def list_projects(self):
        return [i['projectId'] for i in self.cloud_service.projects().list().execute()['projects']]

    def list_service_accounts(self,project):
        resp = self.iam_service.projects().serviceAccounts().list(name='projects/%s' % project,pageSize=100).execute()
        if 'accounts' in resp:
            return resp['accounts']
        return []

    def _generate_id():
        chars = '-abcdefghijklmnopqrstuvwxyz1234567890'
        return 'mg-' + ''.join(choice(chars) for _ in range(26)) + choice(chars[1:])

    def create_projects(self,count):
        if count + len(self.list_projects()) > self.max_projects:
            raise ValueError('Too many projects to create. Max Projects: %s' % self.max_projects)
        batch = self.cloud_service.new_batch_http_request(callback=self._pc_resp)
        new_projs = []
        for i in range(count):
            new_proj = self._generate_id()
            new_projs.append(new_proj)
            batch.add(self.cloud_service.projects().create(body={'project_id':new_proj}))
        batch.execute()

        for i in self.project_create_ops:
            while True:
                resp = self.cloud_service.operations().get(name=i).execute()
                if 'done' in resp and resp['done']:
                    break
                sleep(3)
        return new_projs

    def _pc_resp(self,id,resp,exception):
        if exception is not None:
            print(str(exception))
        else:
            for i in resp.values():
                self.project_create_ops.append(i)

    def create_service_accounts(self,project):
        sa_count = len(self.list_service_accounts(project))
        while sa_count != 100:
            batch = self.iam_service.new_batch_http_request(callback=self._default_batch_resp)
            for i in range(100 - sa_count):
                aid = self._generate_id()
                batch.add(self.iam_service.projects().serviceAccounts().create(name='projects/%s' % project,body={'accountId':aid,'serviceAccount':{'displayName':aid}}))
            batch.execute()
            sa_count = len(self.list_service_accounts(project))

    def delete_service_accounts(self,project):
        batch = self.iam_service.new_batch_http_request(callback=_default_batch_resp)
        for i in self.list_service_accounts(project):
            batch.add(self.iam_service.projects().serviceAccounts().delete(name=i['name']))
        batch.execute()

    def _default_batch_resp(self,id,resp,exception):
        if exception is not None:
            if str(exception).startswith('<HttpError 429'):
                sleep(self.sleep_time/100)
            elif loads(exception.content.decode('utf-8'))['error']['message'] == 'Request had insufficient authentication scopes.':
                raise ValueError('Insufficient authentication scopes.')
            else:
                print(exception)

    def _batch_keys_resp(self,id,resp,exception):
        if exception is not None:
            self.current_key_dump = None
            sleep(self.sleep_time/100)
        elif current_key_dump is None:
            sleep(self.sleep_time/100)
        else:
            self.current_key_dump.append((
                resp['name'][resp['name'].rfind('/'):],
                b64decode(resp['privateKeyData']).decode('utf-8')
            ))

    def enable_services(self,projects,services=['iam','drive']):
        if type(projects) is not list or type(services) is not list:
            raise ValueError('Projects and services must both be lists.')
        services = [i + '.googleapis.com' for i in services]
        batch = self.usage_service.new_batch_http_request(callback=self._default_batch_resp)
        for i in projects:
            for j in services:
                batch.add(self.usage_service.services().enable(name='projects/%s/services/%s' % (i,j)))
        batch.execute()

    def create_service_account_keys(self,project,path='accounts'):
        self.current_key_dump = []
        while self.current_key_dump is None or len(self.current_key_dump) != 100:
            batch = self.iam_service.new_batch_http_request(callback=self._batch_keys_resp)
            total_sas = list_service_accounts(project)
            for j in total_sas:
                batch.add(self.iam_service.projects().serviceAccounts().keys().create(
                    name='projects/%s/serviceAccounts/%s' % (project,j['uniqueId']),
                    body={
                        'privateKeyType':'TYPE_GOOGLE_CREDENTIALS_FILE',
                        'keyAlgorithm':'KEY_ALG_RSA_2048'
                        }))
            batch.execute()
            if self.current_key_dump is None:
                self.current_key_dump = []
            else:
                for j in self.current_key_dump:
                    with open('%s/%s.json' % (path,j[0]),'w+') as f:
                        f.write(j[1])

    def _share_success(self,id,resp,exception):
        if exception is None:
            self.successful.append(resp['emailAddress'])
        else:
            print(str(exception))

    def add_users(self,drive_id,emails):
        while len(self.successful) < len(emails):
            batch = self.drive_service.new_batch_http_request(callback=self._share_success)
            for i in emails:
                if i not in self.successful:
                    batch.add(self.drive_service.permissions().create(fileId=drive_id, fields='emailAddress', supportsAllDrives=True, body={
                        "role": "fileOrganizer",
                        "type": "user",
                        "emailAddress": i
                    }))
            batch.execute()

    def _remove_success(self,id,resp,exception):
        if exception is not None:
            exp = str(exception).split('?')[0].split('/')
            if exp[0].startswith('<HttpError 404'):
                pass
            else:
                self.to_be_removed.append(exp[-1])
        else:
            print(str(exception))

    def remove_users(drive_id,emails=None,role=None,prefix=None,suffix=None):
        if emails is None and role is None and prefix is None and suffix is None:
            raise ValueError('You must provide one of three options: role, prefix, suffix')
        all_perms = []
        rp = self.drive_service.permissions().list(fileId=drive_id,pageSize=100,fields='nextPageToken,permissions(id,emailAddress,role)',supportsAllDrives=True).execute()
        all_perms += rp['permissions']
        while 'nextPageToken' in rp:
            rp = self.drive_service.permissions().list(fileId=drive_id,pageSize=100,fields='nextPageToken,permissions(id,emailAddress,role)',supportsAllDrives=True,pageToken=rp['nextPageToken']).execute()
            all_perms += rp['permissions']

        else:
            for i in all_perms:
                if emails:
                    if i['emailAddress'] in emails:
                        self.to_be_removed.append(i['id'])
                elif role:
                    if role == i['role'].lower():
                        self.to_be_removed.append(i['id'])
                elif prefix:
                    if i['emailAddress'].split('@')[0].startswith(prefix):
                        self.to_be_removed.append(i['id'])
                elif suffix:
                    if i['emailAddress'].split('@')[0].endswith(suffix):
                        self.to_be_removed.append(i['id'])
        while len(self.to_be_removed) > 0:
            # idk why i did this, leaving it here in case its needed later, tbr = [ self.to_be_removed[i:i + 100] for i in range(0, len(self.to_be_removed), 100) ]
            self.to_be_removed = []
            # for j in tbr:
            batch = self.drive_service.new_batch_http_request(callback=self._remove_success)
            for i in self.to_be_removed:
                batch.add(self.drive_service.permissions().delete(fileId=drive_id,permissionId=i,supportsAllDrives=True))
            batch.execute()

if __name__ == '__main__':
    parse = ArgumentParser(description='A tool to create Google service accounts.')
    parse.add_argument('--path',default='accounts',help='Specify an alternate directory to output the credential files. Default: accounts')
    parse.add_argument('--token',default='token.json',help='Specify the token file path. Default: token.json')
    parse.add_argument('--credentials',default='credentials.json',help='Specify the credentials file path. Default: credentials.json')
    parse.add_argument('--max-projects',type=int,default=12,help='Max amount of project allowed. Default: 12')
    parse.add_argument('--sleep',default=30,type=int,help='The amound of seconds to sleep between errors. Default: 30')
    parse.add_argument('--services',nargs='+',default=['iam','drive'],help='Overrides the services to enable. Default: IAM and Drive.')
    parse.add_argument('--remove',default=None,help='Removes users from a Shared Drive..')
    parse.add_argument('-d','--drive-id',default=None,type=str,help='The ID of the Shared Drive.')
    parse.add_argument('--quick-setup',default=None,type=int,help='Create projects, enable services, create service accounts and download keys. ')
    args = parse.parse_args()
    # If no credentials file, search for one.
    if not exists(args.credentials):
        options = glob('*.json')
        print('No credentials found at %s' % args.credentials)
        if len(options) < 1:
            exit(-1)
        else:
            i = 0
            print('Select a credentials file below.')
            inp_options = [str(i) for i in list(range(1,len(options) + 1))] + options
            while i < len(options):
                print('  %d) %s' % (i + 1,options[i]))
                i += 1
            inp = None
            while True:
                inp = input('> ')
                if inp in inp_options:
                    break
            if inp in options:
                args.credentials = inp
            else:
                args.credentials = options[int(inp) - 1]
            if bool(input('Rename %s to credentials.json?')):
                rename(args.credentials,'credentials.json')
                args.credentials = 'credentials.json'
            else:
                print('Use --credentials %s next time to use this credentials file.' % args.credentials)

    # new mg instance
    mg = multimanager(
        token=args.token,
        credentials=args.credentials,
        max_projects=args.max_projects,
        sleep_time=args.sleep
        )

    # quick setup
    try:
        if args.remove:
            assert args.drive_id is not None, 'drive-id is required.'
            opts = args.remove.split('=')
            if opts[0] == 'path':
                accounts_tr = []
                for i in glob('%s/*.json' % opts[1]):
                    accounts_tr.append(loads(open(i,'r').read())['client_email'])
                mg.remove_users(drive_id,emails=accounts_tr)
            elif opts[0] == 'role':
                valid_roles = ['owner','organizer','fileorganizer','writer','reader','commenter']
                valid_levels = ['owner','manager','content manager','contributor','viewer','commenter']
                if opts[1].lower() in valid_levels:
                    opts[1] = valid_roles[valid_levels.index(opts[1].lower())]
                elif opts[1].lower() in valid_roles:
                    opts[1] = opts[1].lower()
                else:
                    print('Invalid role.')
                    exit(-1)
                mg.remove_users(drive_id,role=opts[1])
            elif opts[0] == 'prefix':
                mg.remove_users(drive_id,prefix=opts[1])
            elif opts[0] == 'suffix':
                mg.remove_users(drive_id,suffix=opts[1])
            else:
                print('Invalid input.')

        elif args.quick_setup:
            assert args.drive_id is not None, 'drive_id is required.'
            print('Creating %d projects.' % args.quick_setup)
            projs = mg.create_projects(args.quick_setup)
            print('Enabling services.')
            mg.enable_services(projs,args.services)
            for i in projs:
                print('%s: Creating service accounts.' % i)
                mg.create_service_accounts(i)
                print('%s: Downloading keys.' % i)
                mg.create_service_account_keys(i,path=args.path)
            accounts_to_add = []
            for i in glob('%s/*.json' % args.path):
                accounts_to_add.append(loads(open(i,'r').read())['client_email'])
            mg.add_users(args.drive_id,accounts_to_add)
            print('Done.')
        else:
            print('Nothing to do.')
    except Exception as e:
        print(e)
