from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from _helpers import *
from base64 import b64decode
from os.path import exists
from random import choice
from json import loads
from time import sleep
from uuid import uuid4
from os import rename,mkdir
from glob import glob
from sys import exit
import socket

class BatchJob():
    def __init__(self,service):
        self.batch = service.new_batch_http_request(callback=self.callback_handler)
        self.batch_resp = []
    def add(self,to_add,request_id=None):
        self.batch.add(to_add,request_id=request_id)
    def callback_handler(self,rid,resp,exception):
        response = {'request_id':rid,'exception':None,'response':None}
        if exception is not None:
            response['exception'] = exception
        else:
            response['response'] = resp
        self.batch_resp.append(response)
    def execute(self):
        try:
            self.batch.execute()
        except socket.error:
            pass
        return self.batch_resp

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

    def _generate_id(self):
        chars = '-abcdefghijklmnopqrstuvwxyz1234567890'
        return 'mg-' + ''.join(choice(chars) for _ in range(26)) + choice(chars[1:])

    def _rate_limit_check(self,batch_resp):
        should_sleep = False
        for i in batch_resp:
            if i['exception'] is not None:
                should_sleep = True
                break
        if should_sleep:
            sleep(self.sleep_time)

    def _build_service(self,service,v):
        self.creds = get_creds(
            self.credentials,
            self.token,
            scopes=[
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/iam'])

        return build(service,v,credentials=self.creds)

    def __init__(self,**options):
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
        sas = None
        drives = None
        retries = 0
        self.proj_id = loads(open(self.credentials,'r').read())['installed']['project_id']
        while type(projs) is not list and type(sas) is not list and type(drives) is not list:
            if retries > 5:
                raise RuntimeError('Could not enable required APIs.')
            retries += 1
            try:
                projs = self.list_projects()
                sas = self.list_service_accounts(self.proj_id)
                drives = self.list_shared_drives()
                self.usage_service.services().get(name='projects/%s/services/serviceusage.googleapis.com' % self.proj_id).execute()
            except HttpError as e:
                if loads(e.content.decode('utf-8'))['error']['message'].endswith('If you enabled this API recently, wait a few minutes for the action to propagate to our systems and retry.'):
                    op = self.enable_services([self.proj_id],['drive','iam','cloudresourcemanager'])
                    err = None
                    for i in op:
                        if i['exception'] is not None:
                            err = i['exception']
                            break
                    if err is not None:
                        print(err._get_reason())
                        input('Press Enter to retry.')
                else:
                    raise e

    def list_projects(self):
        return [i['projectId'] for i in self.cloud_service.projects().list().execute()['projects']]

    def list_shared_drives(self):
        all_drives = []
        resp = {'nextPageToken':None}
        while 'nextPageToken' in resp:
            resp = self.drive_service.drives().list(fields='drives(id,name)',pageSize=100,pageToken=resp['nextPageToken']).execute()
            all_drives += resp['drives']
        return all_drives

    def list_service_accounts(self,project):
        try:
            resp = self.iam_service.projects().serviceAccounts().list(name='projects/%s' % project,pageSize=100).execute()
        except HttpError as e:
            if loads(e.content.decode('utf-8'))['error']['message'] == 'The caller does not have permission':
                raise RuntimeError('Could not list Service Accounts in project %s' % project)
        if 'accounts' in resp:
            return resp['accounts']
        return []

    def create_projects(self,count):
        if count + len(self.list_projects()) > self.max_projects:
            raise ValueError('Too many projects to create. Max Projects: %s' % self.max_projects)
        batch = BatchJob(self.cloud_service)
        new_projs = []
        for i in range(count):
            new_proj = self._generate_id()
            new_projs.append(new_proj)
            batch.add(self.cloud_service.projects().create(body={'project_id':new_proj}))
        project_create_ops = batch.execute()

        for i in project_create_ops:
            while True:
                resp = self.cloud_service.operations().get(name=i['response']['name']).execute()
                if 'done' in resp and resp['done']:
                    break
                sleep(3)
        return new_projs

    def create_shared_drive(self,name):
        try:
            return self.drive_service.drives().create(body={'name': name},requestId=str(uuid4()),fields='id,name').execute()
        except HttpError as e:
            if loads(e.content.decode('utf-8'))['error']['message'] == 'The user does not have sufficient permissions for this file.':
                raise ValueError('User cannot create Shared Drives.')
            else:
                raise e

    def create_service_accounts(self,project):
        sa_count = len(self.list_service_accounts(project))
        while sa_count != 100:
            batch = BatchJob(self.iam_service)
            for i in range(100 - sa_count):
                aid = self._generate_id()
                batch.add(self.iam_service.projects().serviceAccounts().create(name='projects/%s' % project,body={'accountId':aid,'serviceAccount':{'displayName':aid}}))
            self._rate_limit_check(batch.execute())
            sa_count = len(self.list_service_accounts(project))

    def create_service_account_keys(self,project,path='accounts'):
        current_key_dump = []
        total_sas = self.list_service_accounts(project)
        try:
            mkdir(path)
        except FileExistsError:
            pass
        while current_key_dump is None or len(current_key_dump) != 100:
            batch = BatchJob(self.iam_service)
            for j in total_sas:
                batch.add(self.iam_service.projects().serviceAccounts().keys().create(
                    name='projects/%s/serviceAccounts/%s' % (project,j['uniqueId']),
                    body={
                        'privateKeyType':'TYPE_GOOGLE_CREDENTIALS_FILE',
                        'keyAlgorithm':'KEY_ALG_RSA_2048'
                        }))
            current_key_dump = batch.execute()

            retry = False
            for i in current_key_dump:
                if i['exception'] is not None:
                    retry = True
                    break

            if not retry:
                for i in current_key_dump:
                    with open('%s/%s.json' % (path,i['response']['name'][i['response']['name'].rfind('/'):]),'w+') as f:
                        f.write(b64decode(i['response']['privateKeyData']).decode('utf-8'))

    def enable_services(self,projects,services=['iam','drive']):
        if type(projects) is not list or type(services) is not list:
            raise ValueError('Projects and services must both be lists.')
        services = [i + '.googleapis.com' for i in services]
        batch = BatchJob(self.usage_service)
        for i in projects:
            for j in services:
                batch.add(self.usage_service.services().enable(name='projects/%s/services/%s' % (i,j)),request_id='%s|%s' % (j,i))
        enable_ops = batch.execute()

        for i in enable_ops:
            if i['exception'] is not None:
                prj_and_serv =i['request_id'].split('|')
                raise RuntimeError('Could not enable the service %s on the project %s.' % (prj_and_serv[0],prj_and_serv[1]))

        return True

    def delete_service_accounts(self,project):
        batch = BatchJob(self.iam_service)
        for i in self.list_service_accounts(project):
            batch.add(self.iam_service.projects().serviceAccounts().delete(name=i['name']))
        self._rate_limit_check(batch.execute())
        sas = self.list_service_accounts(project)
        sa_count = len(sas)
        while sa_count != 0:
            batch = BatchJob(self.iam_service)
            for i in sas:
                batch.add(self.iam_service.projects().serviceAccounts().delete(name=i['name']))
            self._rate_limit_check(batch.execute())
            sas = self.list_service_accounts(project)
            sa_count = len(sas)

    def add_users(self,drive_id,emails):
        successful = []
        while len(successful) < len(emails):
            batch = BatchJob(self.drive_service)
            for i in emails:
                if i not in successful:
                    batch.add(self.drive_service.permissions().create(fileId=drive_id, fields='emailAddress', supportsAllDrives=True, body={
                        'role': 'fileOrganizer',
                        'type': 'user',
                        'emailAddress': i
                    }))
            for i in batch.execute():
                if i['exception'] is None:
                    successful.append(i['response']['emailAddress'])

    def remove_users(drive_id,emails=None,role=None,prefix=None,suffix=None):
        if emails is None and role is None and prefix is None and suffix is None:
            raise ValueError('You must provide one of three options: role, prefix, suffix')
        all_perms = []
        rp = {'nextPageToken':None}
        while 'nextPageToken' in rp:
            rp = self.drive_service.permissions().list(fileId=drive_id,pageSize=100,fields='nextPageToken,permissions(id,emailAddress,role)',supportsAllDrives=True,pageToken=rp['nextPageToken']).execute()
            all_perms += rp['permissions']

        else:
            for i in all_perms:
                if emails:
                    if i['emailAddress'] in emails:
                        to_be_removed.append(i['id'])
                elif role:
                    if role == i['role'].lower():
                        to_be_removed.append(i['id'])
                elif prefix:
                    if i['emailAddress'].split('@')[0].startswith(prefix):
                        to_be_removed.append(i['id'])
                elif suffix:
                    if i['emailAddress'].split('@')[0].endswith(suffix):
                        to_be_removed.append(i['id'])
        while len(to_be_removed) > 0:
            batch = BatchJob(self.drive_service)
            for i in to_be_removed:
                batch.add(self.drive_service.permissions().delete(fileId=drive_id,permissionId=i,supportsAllDrives=True))
            to_be_removed = []
            for i in batch.execute():
                if i['exception'] is not None:
                    exp = str(i['exception']).split('?')[0].split('/')
                    if not exp[0].startswith('<HttpError 404'):
                        to_be_removed.append(exp[-1])

# args handler
def args_handler(mg,args):
    try:
        if args.command == 'quick-setup':
            if args.amount < 1 and args.amount < 6:
                print('multimanager.py create projects: error: the following arguments must be greater than 0 and less thatn 6: amount')
            else:
                print('Creating %d projects.' % args.amount)
                projs = mg.create_projects(args.amount)
                print('Enabling services.')
                mg.enable_services(projs)
                for i in projs:
                    print('Creating Service Accounts in %s' % i)
                    mg.create_service_accounts(i)
                    print('Creating Service Account keys in %s' % i)
                    mg.create_service_account_keys(i,path=args.path)
                accounts_to_add = []
                print('Fetching emails.')
                for i in glob('%s/*.json' % args.path):
                    accounts_to_add.append(loads(open(i,'r').read())['client_email'])
                print('Adding %d users' % len(accounts_to_add))
                mg.add_users(args.drive_id,accounts_to_add)
                print('Done.')

        # list
        elif args.command == 'list':

            # list drives
            if args.list == 'drives':
                drives = mg.list_shared_drives()
                if len(drives) < 1:
                    print('No Shared Drives found.')
                else:
                    print('Shared Drives (%d):' % len(drives))
                    for i in drives:
                        print('  %s (ID: %s)' % (i['name'],i['id']))

            # list projects
            elif args.list == 'projects':
                projs = mg.list_projects()
                if len(projs) < 1:
                    print('No projects found.')
                else:
                    print('Projects (%d):' % len(projs))
                    for i in projs:
                        print('  %s' % (i))

            # list accounts PROJECTS
            elif args.list == 'accounts':
                if len(args.project) == 1 and args.project[0] == 'all':
                    args.project = mg.list_projects()
                sas = []
                for i in args.project:
                    sas += mg.list_service_accounts(i)
                if len(sas) < 1:
                    print('No Service Accounts found.')
                else:
                    for i in sas:
                        print('  %s (%s)' % (i['email'],i['uniqueId']))

        # create
        elif args.command == 'create':

            # create projects N
            if args.list == 'projects':
                if args.amount < 1:
                    print('multimanager.py create projects: error: the following arguments must be greater than 0: amount')
                else:
                    projs = mg.create_projects(args.amount)
                    print('New Projects (%d):' % len(projs))
                    for i in projs:
                        print('  %s' % i)

            # create drive NAME
            if args.list == 'drive':
                newsd = mg.create_shared_drive(args.name)
                print('Shared Drive Name: %s\n  Shared Drive ID: %s' % (newsd['name'],newsd['id']))

            # create accounts PROJECTS
            if args.list == 'accounts':
                if len(args.project) == 1 and args.project[0] == 'all':
                    args.project = mg.list_projects()
                for i in args.project:
                    print('Creating Service Accounts in %s' % i)
                    mg.create_service_accounts(i)

            # create account-keys PROJECTS
            if args.list == 'account-keys':
                if len(args.project) == 1 and args.project[0] == 'all':
                    args.project = mg.list_projects()
                for i in args.project:
                    print('Creating Service Accounts Keys in %s' % i)
                    mg.create_service_account_keys(i,path=args.path)

        # enable-services PROJECTS
        elif args.command == 'enable-services':
            if len(args.project) == 1 and args.project[0] == 'all':
                args.project = mg.list_projects()
            outptstr = 'Enabling services (%d):\n' % len(args.services)
            for i in args.services:
                outptstr += '  %s\n' % i
            outptstr += 'On projects (%d):\n' % len(args.project)
            for i in args.project:
                outptstr += '  %s\n' % i
            print(outptstr[:-1])
            mg.enable_services(args.project)
            print('Services enabled.')

        # delete PROJECTS (deletes accounts from PROJECTS)
        elif args.command == 'delete':
            if len(args.project) == 1 and args.project[0] == 'all':
                args.project = mg.list_projects()
            for i in args.project:
                print('Deleting Service Accounts in %s' % i)
                mg.delete_service_accounts(i)

        # add DRIVE_ID (add users from args.path to the drive)
        elif args.command == 'add':
            accounts_to_add = []
            for i in glob('%s/*.json' % args.path):
                accounts_to_add.append(loads(open(i,'r').read())['client_email'])
            if len(accounts_to_add) > 599:
                print('More than 599 accounts detected. Shared Drives can only hold 600 users max. Split the accounts into smaller folders and specify the path using the --path flag.')
            else:
                print('Adding %d users' % len(accounts_to_add))
                mg.add_users(args.drive_id,accounts_to_add)

        # remove DRIVE_ID (remove users from the drive)
        elif args.command == 'remove':

            # remove DRIVE_ID pattern ROLE
            if args.pattern_type == 'role':
                if args.pattern_type.lower() in ('owner','organizer','fileorganizer','writer','reader','commenter'):
                    mg.remove_users(args.drive_id,role=args.pattern)
                else:
                    pring('Invalid role %s. Choose from (owner,organizer,fileorganizer,writer,reader,commenter)' % args.pattern)

            # remove DRIVE_ID pattern SUFFIX
            elif args.pattern_type == 'suffix':
                mg.remove_users(args.drive_id,suffix=args.pattern)

            # remove DRIVE_ID pattern ROLE
            elif args.pattern_type == 'prefix':
                mg.remove_users(args.drive_id,prefix=args.pattern)
    except Exception as e:
        print(e)

if __name__ == '__main__':
    from argparse import ArgumentParser

    # master parser
    parse = ArgumentParser(description='A multi-purpose manager for Shared Drives and Google Cloud.')

    # opts
    parse.add_argument('--sleep',default=30,type=int,help='The amound of seconds to sleep between rate limit errors. Default: 30')
    parse.add_argument('--path',default='accounts',help='Specify an alternate directory to output the credential files. Default: accounts')
    parse.add_argument('--max-projects',type=int,default=12,help='Max amount of project allowed. Default: 12')
    parse.add_argument('--services',action='append',help='Overrides the services to enable. Default: IAM and Drive.')

    # google auth options
    auth = parse.add_argument_group('Google Authentication')
    auth.add_argument('--token',default='token.json',help='Specify the token file path. Default: token.json')
    auth.add_argument('--credentials',default='credentials.json',help='Specify the credentials file path. Default: credentials.json')

    # command subparsers
    subparsers = parse.add_subparsers(help='Commands',dest='command',required=True)
    ls = subparsers.add_parser('list',help='List items in a resource.')
    create = subparsers.add_parser('create',help='Creates a new resource.')
    enable = subparsers.add_parser('enable-services',help='Enable services in a project.')
    delete = subparsers.add_parser('delete',help='Delete a resource.')
    add = subparsers.add_parser('add',help='Add users to a Shared Drive.')
    remove = subparsers.add_parser('remove',help='Remove users from a Shared Drive.')
    quicksetup = subparsers.add_parser('quick-setup',help='Runs a quick setup for folderclone.')
    interact = subparsers.add_parser('interactive',help='Initiate an interactive Multi Manager instance.')

    # ls
    lsparsers = ls.add_subparsers(help='List options.',dest='list',required=True)
    lsprojects = lsparsers.add_parser('projects',help='List projects viewable by the user.')
    lsdrive = lsparsers.add_parser('drives',help='List Shared Drives viewable by the user.')
    lsaccounts = lsparsers.add_parser('accounts',help='List Shared Drives viewable by the user.')
    lsaccounts.add_argument('project',nargs='+',help='List Service Accounts in a project.')

    # create
    createparse = create.add_subparsers(help='List options.',dest='list',required=True)

    createprojs = createparse.add_parser('projects',help='Create new projects.')
    createprojs.add_argument('amount',type=int,help='The amount of projects to create.')

    createdrive = createparse.add_parser('drive',help='Create a new Shared Drive.')
    createdrive.add_argument('name',help='The name of the new Shared Drive.')

    createaccounts = createparse.add_parser('accounts',help='Create Service Accounts in a project.')
    createaccounts.add_argument('project',nargs='+',help='Project in which to create Service Accounts in.')

    createkeys = createparse.add_parser('account-keys',help='List Shared Drives viewable by the user.')
    createkeys.add_argument('project',nargs='+',help='Project in which to create Service Account keys in.')

    # remove
    remove.add_argument(metavar='Drive ID',dest='drive_id',help='The ID of the Shared Drive to remove users from.')
    remove.add_argument(metavar='pattern',dest='pattern_type',choices=('prefix','suffix','role'),help='Remove users by prefix/suffix/role.')
    remove.add_argument(metavar='prefix/suffix/role',dest='pattern',help='The prefix/suffix/role of the users you want to remove.')

    # add
    add.add_argument(metavar='Drive ID',dest='drive_id',help='The ID of the Shared Drive to add users to.')

    # delete
    deleteparse = delete.add_subparsers(metavar='resource',help='Delete options.',dest='delete',required=True)

    deleteaccounts = deleteparse.add_parser('accounts',help='Delete Service Accounts.')
    deleteaccounts.add_argument('project',nargs='+',help='The project to delete Service Accounts from.')

    # enable-services
    enable.add_argument('project',nargs='+',help='The project in which to enable services.')

    # folderclone quick setup
    quicksetup.add_argument('amount',type=int,help='The amount of projects to create for use with folderclone.')
    quicksetup.add_argument(metavar='Drive ID',dest='drive_id',help='The ID of the Shared Drive to use for folderclone.')

    args = parse.parse_args()
    args.services = ['iam','drive'] if args.services is None else args.services

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
                inp = input('mm> ')
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
        sleep_time=args.sleep,
        max_projects=args.max_projects
        )

    # interactive
    if args.command == 'interactive':
        from sys import platform
        if platform != 'win32:
            import readline
        inp = ['']
        print('Multi Manager v0.5.0')
        while inp[0] != 'exit':
            try:
                inp = input('mm> ').strip().split()
            except KeyboardInterrupt:
                inp = []
                print()
            except EOFError:
                inp = ['exit']
            if len(inp) < 1:
                inp = ['']
            elif inp[0] == 'interactive':
                print('Already in interactive mode.')
            elif inp[0] != 'exit':
                if inp[0] == 'help':
                    inp[0] = '--help'
                try:
                    args_handler(mg,parse.parse_args(inp))
                except SystemExit as e:
                    pass
    else:
        args_handler(mg,args)
