from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from folderclone._helpers import *
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
    sleep_time = 30

    def _generate_id(self,prefix=None,p=None):
        chars = '-abcdefghijklmnopqrstuvwxyz1234567890'
        if prefix is None:
            prefix = 'mm-'
        if p is None:
            p = 29 - len(prefix)
        return prefix + ''.join(choice(chars) for _ in range(p)) + choice(chars[1:])

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
                        err_msg = err._get_reason()
                        from webbrowser import open_new_tab
                        open_new_tab(err_msg[err_msg.find('visiting')+9:err_msg.find('then')-1])
                        print(err_msg)
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
            if 'accounts' in resp:
                return resp['accounts']
            return []
        except HttpError as e:
            if loads(e.content.decode('utf-8'))['error']['message'] == 'The caller does not have permission':
                raise RuntimeError('Could not list Service Accounts in project %s' % project)
            else:
                raise e

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

    def create_service_accounts(self,project,prefix=None,p=None):
        sa_count = len(self.list_service_accounts(project))
        while sa_count != 100:
            batch = BatchJob(self.iam_service)
            for i in range(100 - sa_count):
                aid = self._generate_id(prefix=prefix,p=p)
                batch.add(self.iam_service.projects().serviceAccounts().create(name='projects/%s' % project,fields='',body={'accountId':aid,'serviceAccount':{'displayName':aid}}))
            self._rate_limit_check(batch.execute())
            sa_count = len(self.list_service_accounts(project))

    def create_service_account_keys(self,project,path='accounts'):
        current_key_dump = []
        total_sas = [i['uniqueId'] for i in self.list_service_accounts(project)]
        try:
            mkdir(path)
        except FileExistsError:
            pass
        while len(total_sas) != 0:
            batch = BatchJob(self.iam_service)
            for j in total_sas:
                batch.add(self.iam_service.projects().serviceAccounts().keys().create(
                    name='projects/%s/serviceAccounts/%s' % (project,j),
                    body={
                        'privateKeyType':'TYPE_GOOGLE_CREDENTIALS_FILE',
                        'keyAlgorithm':'KEY_ALG_RSA_2048'
                        }),request_id=j)
            current_key_dump = batch.execute()

            for i in current_key_dump:
                if i['exception'] is None:
                    total_sas.remove(i['request_id'])
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
        return batch.execute()

    def delete_service_accounts(self,project):
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
        while len(emails) > 0:
            resp = []
            for spl in [emails[i:i + 100] for i in range(0, len(emails), 100)]:
                batch = BatchJob(self.drive_service)
                for i in spl:
                    batch.add(self.drive_service.permissions().create(fileId=drive_id, fields='emailAddress', supportsAllDrives=True, body={
                        'role': 'fileOrganizer',
                        'type': 'user',
                        'emailAddress': i
                    }),i)
                resp += batch.execute()
            emails = []
            for i in resp:
                if i['exception'] is not None:
                    emails.append(i['request_id'])
                else:
                    sleep(self.sleep_time/len(resp))

    def remove_users(self,drive_id,emails=None,role=None,prefix=None,suffix=None):
        if emails is None and role is None and prefix is None and suffix is None:
            raise ValueError('You must provide one of three options: role, prefix, suffix')
        all_perms = []
        rp = {'nextPageToken':None}
        while 'nextPageToken' in rp:
            rp = self.drive_service.permissions().list(fileId=drive_id,pageSize=100,fields='nextPageToken,permissions(id,emailAddress,role)',supportsAllDrives=True,pageToken=rp['nextPageToken']).execute()
            all_perms += rp['permissions']

        to_be_removed = []
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
        to_be_removed = list(set(to_be_removed))

        while len(to_be_removed) > 0:
            resp = []
            for spl in [to_be_removed[i:i + 100] for i in range(0, len(to_be_removed), 100)]:
                batch = BatchJob(self.drive_service)
                for i in spl:
                    batch.add(self.drive_service.permissions().delete(fileId=drive_id,permissionId=i,supportsAllDrives=True),i)
                resp += batch.execute()
            to_be_removed = []
            for i in resp:
                if i['exception'] is not None:
                    if not str(i['exception']).startswith('<HttpError 404'):
                        to_be_removed.append(i['request_id'])
                    else:
                        sleep(self.sleep_time/len(resp))
            to_be_removed = list(set(to_be_removed))
