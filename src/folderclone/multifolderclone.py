from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from urllib3.exceptions import ProtocolError
from googleapiclient.discovery import build
from google.auth.exceptions import TransportError
from httplib2shim import patch
from glob import glob
import time,threading,json,socket

class multifolderclone():
    patch()
    source = ''
    dest = []
    path = 'accounts'
    width = 2
    thread_count = None
    override_thread_check = False
    skip_bad_dests = False

    drive_to_use = 1
    files_to_copy = []
    threads = None
    id_whitelist = None
    id_blacklist = None
    name_whitelist = None
    name_blacklist = None
    bad_drives = []
    google_opts = ['trashed = false']
    max_retries = 3
    sleep_time = 1
    verbose = False

    error_codes = {
        'dailyLimitExceeded': True,
        'userRateLimitExceeded': True,
        'rateLimitExceeded': True,
        'sharingRateLimitExceeded': True,
        'appNotAuthorizedToFile': True,
        'insufficientFilePermissions': True,
        'domainPolicy': True,
        'backendError': True,
        'internalError': True,
        'badRequest': False,
        'invalidSharingRequest': False,
        'authError': False,
        'notFound': False
    }

    def __init__(self,source,dest,**options):
        self.source = source
        self.dest = dest
        if type(dest) is str:
            self.dest = [dest]
        if options.get('thread_count') is not None:
            self.thread_count = int(options['thread_count'])
        if options.get('skip_bad_dests') is not None:
            self.skip_bad_dests = bool(options['skip_bad_dests'])
        if options.get('path') is not None:
            self.path = str(options['path'])
        if options.get('width') is not None:
            self.width = int(options['width'])
        if options.get('sleep_time') is not None:
            self.sleep_time = int(options['sleep_time'])
        if options.get('max_retries') is not None:
            self.max_retries = int(options['max_retries'])
        if options.get('id_whitelist') is not None:
            self.id_whitelist = list(options['id_whitelist'])
        if options.get('name_whitelist') is not None:
            self.name_whitelist = list(options['name_whitelist'])
        if options.get('id_blacklist') is not None:
            self.id_blacklist = list(options['id_blacklist'])
        if options.get('name_blacklist') is not None:
            self.name_blacklist = list(options['name_blacklist'])
        if options.get('override_thread_check') is not None:
            self.override_thread_check = bool(options['override_thread_check'])
        if options.get('verbose') is not None:
            self.verbose = bool(options['verbose'])
        if options.get('google_opts') is not None:
            google_opts = list(google_opts)
    def _log(self,s):
        if self.verbose:
            print(s)

    def _apicall(self,request):
        resp = None
        tries = 0

        while True:
            tries += 1
            if tries > self.max_retries:
                self._log('Could not copy.')
                return None
            try:
                resp = request.execute()
            except HttpError as error:
                self._log(str(error))
                try:
                    error_details = json.loads(error.content.decode('utf-8'))
                except json.decoder.JSONDecodeError:
                    time.sleep(self.sleep_time)
                    continue
                reason = error_details['error']['errors'][0]['reason']
                if reason == 'userRateLimitExceeded':
                    return False
                elif reason == 'storageQuotaExceeded':
                    print('Got storageQuotaExceeded error. You are not using a Shared Drive.')
                    return False
                elif reason == 'teamDriveFileLimitExceeded':
                    raise RuntimeError('The Shared Drive is full. No more files can be copied to it.')
                elif self.error_codes[reason]:
                    time.sleep(self.sleep_time)
                    continue
                else:
                    return None
            except (socket.error, ProtocolError, TransportError):
                time.sleep(self.sleep_time)
                continue
            else:
                return resp

    def _ls(self,service,parent, searchTerms=[]):
        files = []
        resp = {'nextPageToken':None}
        while 'nextPageToken' in resp:
            resp = self._apicall(
                service.files().list(
                    q=' and '.join(['"%s" in parents' % parent] + self.google_opts + self.searchTermsΩ),
                    fields='files(md5Checksum,id,name),nextPageToken',
                    pageSize=1000,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    pageToken=resp['nextPageToken']
                )
            )
            files += resp['files']
        return files

    def _lsd(self,service,parent):
        return self._ls(
            service,
            parent,
            searchTerms=['mimeType contains "application/vnd.google-apps.folder"']
        )

    def _lsf(self,service,parent):
        return self._ls(
            service,
            parent,
            searchTerms=['not mimeType contains "application/vnd.google-apps.folder"']
        )

    def _copy(self,driv,source,dest):
        self._log('Copying file %s into folder %s' % (source,dest))
        if self._apicall(driv.files().copy(fileId=source, body={'parents': [dest]}, supportsAllDrives=True)) == False:
            self._log('Error: Quotad SA')
            self.bad_drives.append(driv)
            self.files_to_copy.append((source,dest))
        self.threads.release()
             
    def _rcopy(self,drive,drive_to_use,source,dest,folder_name,display_line,width):
        print('%s to %s' % (source,dest))
        files_source = self._lsf(drive[0],source)
        files_dest = self._lsf(drive[0],dest)
        folders_source = self._lsd(drive[0],source)
        folders_dest = self._lsd(drive[0],dest)
        self._log('Found %d files in source.' % len(files_source))
        self._log('Found %d folders in source.' % len(folders_source))
        self._log('Found %d files in dest.' % len(files_dest))
        self._log('Found %d folders in dest.' % len(folders_dest))
        files_to_copy = []
        files_source_id = []
        files_dest_id = []

        folder_len = len(folders_source) - 1

        folders_copied = {}
        for file in files_source:
            files_source_id.append(dict(file))
            file.pop('id')
        for file in files_dest:
            files_dest_id.append(dict(file))
            file.pop('id')

        i = 0
        while len(files_source) > i:
            if files_source[i] not in files_dest:
                files_to_copy.append(files_source_id[i])
            i += 1
        self._log('Added %d files to copy list.' % len(files_to_copy))

        for i in list(files_to_copy):
            if self.id_whitelist is not None:
                if i['id'] not in self.id_whitelist:
                    files_to_copy.remove(i)
            if self.id_blacklist is not None:
                if i['id'] in self.id_blacklist:
                    files_to_copy.remove(i)
            if self.name_whitelist is not None:
                if i['name'] not in self.name_whitelist:
                    files_to_copy.remove(i)
            if self.name_blacklist is not None:
                if i['name'] in self.name_blacklist:
                    files_to_copy.remove(i)

        self.files_to_copy = [ (i['id'],dest) for i in files_to_copy ]

        self._log('Copying files')
        if len(files_to_copy) > 0:
            while len(self.files_to_copy) > 0:
                files_to_copy = self.files_to_copy
                self.files_to_copy = []
                running_threads = []

                # copy
                for i in files_to_copy:
                    self.threads.acquire()
                    thread = threading.Thread(
                        target=self._copy,
                        args=(drive[drive_to_use],i[0],i[1])
                    )
                    running_threads.append(thread)
                    thread.start()
                    drive_to_use += 1
                    if drive_to_use > len(drive) - 1:
                        drive_to_use = 1

                # join all threads
                for i in running_threads:
                    i.join()

                # check for bad drives
                for i in self.bad_drives:
                    if i in drive:
                        drive.remove(i)
                self.bad_drives = []

                # If there is less than 2 SAs, exit
                if len(drive) == 1:
                    raise RuntimeError('Out of SAs.')

            # copy completed
            print(display_line + folder_name + ' | Synced')
        elif len(files_source) > 0 and len(files_source) <= len(files_dest):
            print(display_line + folder_name + ' | Up to date')
        else:
            print(display_line + folder_name)

        for i in folders_dest:
            folders_copied[i['name']] = i['id']
        
        current_folder = 0
        for folder in folders_source:
            if current_folder == folder_len:
                next_display_line = display_line.replace('├' + '─' * width + ' ', '│' + ' ' * width + ' ').replace('└' + '─' * width + ' ', '  ' + ' ' * width) + '└' + '─' * width + ' '
            else:
                next_display_line = display_line.replace('├' + '─' * width + ' ', '│' + ' ' * width + ' ').replace('└' + '─' * width + ' ', '  ' + ' ' * width) + '├' + '─' * width + ' '
            if folder['name'] not in folders_copied.keys():
                folder_id = self._apicall(
                    drive[0].files().create(
                        body={
                            'name': folder['name'],
                            'mimeType': 'application/vnd.google-apps.folder',
                            'parents': [dest]
                        },
                        supportsAllDrives=True
                    )
                )['id']
            else:
                folder_id = folders_copied[folder['name']]
            drive = self._rcopy(
                drive,
                drive_to_use,
                folder['id'],
                folder_id,
                folder['name'].replace('%', '%%'),
                next_display_line,
                width
            )
            current_folder += 1
        return drive

    def clone(self):
        accounts = glob(self.path + '/*.json')
        if len(accounts) < 2:
            raise ValueError('The path provided (%s) has 1 or no accounts.' % self.path)

        check = build('drive','v3',credentials=Credentials.from_service_account_file(accounts[0]))

        try:
            root_dir = check.files().get(fileId=self.source,supportsAllDrives=True).execute()['name']
        except HttpError:
            raise ValueError('Source folder %s cannot be read or is invalid.' % self.source)

        dest_dict = {i:'' for i in self.dest}
        for key in list(dest_dict.keys()):
            try:
                dest_dir = check.files().get(fileId=key,supportsAllDrives=True).execute()['name']
                dest_dict[key] = dest_dir
            except HttpError:
                if not skip_bad_dests:
                    raise ValueError('Destination folder %s cannot be read or is invalid.' % key)
                else:
                    dest_dict.pop(key)

        print('Creating %d Drive Services' % len(accounts))
        drive = []
        for account in accounts:
            credentials = Credentials.from_service_account_file(account, scopes=[
                'https://www.googleapis.com/auth/drive'
            ])
            drive.append(build('drive', 'v3', credentials=credentials))
        if self.thread_count is not None and (self.override_thread_check or self.thread_count <= len(drive)):
            self.threads = threading.BoundedSemaphore(self.thread_count)
            print('BoundedSemaphore with %d threads' % self.thread_count)
        elif self.thread_count is None:
            self.threads = threading.BoundedSemaphore(len(drive))
            print('BoundedSemaphore with %d threads' % len(drive))
        else:
            raise ValueError('More threads than there is service accounts.')

        for i, dest_dir in dest_dict.items():
            print('Copying from %s to %s.' % (root_dir,dest_dir))
            self._rcopy(drive,1,self.source,i,root_dir,'',self.width)
