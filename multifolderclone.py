from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from urllib3.exceptions import ProtocolError
from googleapiclient.discovery import build
from argparse import ArgumentParser
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
    skip_bad_dests = False

    dtu = 1
    retry = []
    threads = None
    bad_drives = []
    max_retries = 3
    sleep_time = 1

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

    def _apicall(self,request):
        resp = None
        tries = 0

        while True:
            tries += 1
            if tries > self.max_retries:
                return None
            try:
                resp = request.execute()
            except HttpError as error:
                try:
                    error_details = json.loads(error.content.decode("utf-8"))
                except json.decoder.JSONDecodeError:
                    time.sleep(self.sleep_time)
                    continue
                reason = error_details["error"]["errors"][0]["reason"]
                if reason == 'userRateLimitExceeded':
                    return False
                elif reason == 'storageQuotaExceeded':
                    print('Got storageQuotaExceeded error. You are not using a Shared Drive.')
                    return False
                elif self.error_codes[reason]:
                    time.sleep(self.sleep_time)
                    continue
                else:
                    return None
            except (socket.error, ProtocolError):
                time.sleep(self.sleep_time)
                continue
            else:
                return resp

    def _ls(self,service,parent, searchTerms=""):
        files = []
        
        resp = self._apicall(
            service.files().list(
                q="'%s' in parents%s" % (parent,searchTerms),
                fields='files(md5Checksum,id,name),nextPageToken',
                pageSize=1000,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            )
        )
        files += resp["files"]

        while "nextPageToken" in resp:
            resp = self._apicall(
                service.files().list(
                    q="'%s' in parents%s" % (parent,searchTerms),
                    fields='files(md5Checksum,id,name),nextPageToken',
                    pageSize=1000,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    pageToken=resp["nextPageToken"]
                )
            )
            files += resp["files"]
        return files

    def _lsd(self,service,parent):
        return self._ls(
            service,
            parent,
            searchTerms=" and mimeType contains 'application/vnd.google-apps.folder'"
        )

    def _lsf(self,service,parent):
        return self._ls(
            service,
            parent,
            searchTerms=" and not mimeType contains 'application/vnd.google-apps.folder'"
        )

    def _copy(self,driv,source,dest):
        if self._apicall(driv.files().copy(fileId=source, body={"parents": [dest]}, supportsAllDrives=True)) == False:
            self.bad_drives.append(driv)
            self.retry.append((source,dest))
        self.threads.release()
             
    def _rcopy(self,drive,dtu,source,dest,sname,pre,width):
        pres = pre
        files_source = self._lsf(drive[0],source)
        files_dest = self._lsf(drive[0],dest)
        folders_source = self._lsd(drive[0],source)
        folders_dest = self._lsd(drive[0],dest)
        files_to_copy = []
        files_source_id = []
        files_dest_id = []

        fs = len(folders_source) - 1

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
        for i in self.retry:
            self.threads.acquire()
            thread = threading.Thread(
                target=self._copy,
                args=(
                    drive[dtu],
                    i[0],
                    i[1]
                )
            )
            thread.start()
            dtu += 1
            if dtu > len(drive) - 1:
                dtu = 1
        self.retry = []
        if len(files_to_copy) > 0:
            for file in files_to_copy:
                self.threads.acquire()
                thread = threading.Thread(
                    target=self._copy,
                    args=(
                        drive[dtu],
                        file['id'],
                        dest
                    )
                )
                thread.start()
                dtu += 1
                if dtu > len(drive) - 1:
                    dtu = 1
            print(pres + sname + ' | Synced')
        elif len(files_source) > 0 and len(files_source) <= len(files_dest):
            print(pres + sname + ' | Up to date')
        else:
            print(pres + sname)
        for i in self.bad_drives:
            if i in drive:
                drive.remove(i)
        self.bad_drives = []
        if len(drive) == 1:
            print('Out of SAs.')
            return

        for i in folders_dest:
            folders_copied[i['name']] = i['id']
        
        s = 0
        for folder in folders_source:
            if s == fs:
                nstu = pre.replace("├" + "─" * width + " ", "│" + " " * width + " ").replace("└" + "─" * width + " ", "  " + " " * width) + "└" + "─" * width + " "
            else:
                nstu = pre.replace("├" + "─" * width + " ", "│" + " " * width + " ").replace("└" + "─" * width + " ", "  " + " " * width) + "├" + "─" * width + " "
            if folder['name'] not in folders_copied.keys():
                folder_id = _apicall(
                    drive[0].files().create(
                        body={
                            "name": folder["name"],
                            "mimeType": "application/vnd.google-apps.folder",
                            "parents": [dest]
                        },
                        supportsAllDrives=True
                    )
                )['id']
            else:
                folder_id = folders_copied[folder['name']]
            drive = self._rcopy(
                drive,
                dtu,
                folder["id"],
                folder_id,
                folder["name"].replace('%', "%%"),
                nstu,
                width
            )
            s += 1
        return drive

    def clone(self):
        accounts = glob(self.path + '/*.json')

        check = build("drive", "v3", credentials=Credentials.from_service_account_file(accounts[0]))

        try:
            root_dir = check.files().get(fileId=self.source, supportsAllDrives=True).execute()['name']
        except HttpError:
            raise ValueError('Source folder %s cannot be read or is invalid.' % self.source)

        dest_dict = {i:'' for i in self.dest}
        for key in list(dest_dict.keys()):
            try:
                dest_dir = check.files().get(fileId=key, supportsAllDrives=True).execute()['name']
                dest_dict[key] = dest_dir
            except HttpError:
                if not skip_bad_dests:
                    raise ValueError('Destination folder %s cannot be read or is invalid.' % key)
                else:
                    dest_dict.pop(key)

        print("Creating %d Drive Services" % len(accounts))
        drive = []
        for account in accounts:
            credentials = Credentials.from_service_account_file(account, scopes=[
                "https://www.googleapis.com/auth/drive"
            ])
            drive.append(build("drive", "v3", credentials=credentials))
        if self.thread_count is not None and self.thread_count <= len(drive):
            self.threads = threading.BoundedSemaphore(self.thread_count)
            print('BoundedSemaphore with %d threads' % self.thread_count)
        elif self.thread_count is None:
            self.threads = threading.BoundedSemaphore(len(drive))
            print('BoundedSemaphore with %d threads' % len(drive))
        else:
            raise ValueError('More threads than there is service accounts.')

        for i, dest_dir in dest_dict.items():
            print('Copying from %s to %s.' % (root_dir, dest_dir))
            self._rcopy(drive, 1,self.source, i, root_dir, "", self.width)

if __name__ == '__main__':
    parse = ArgumentParser(description='A tool intended to copy large files from one folder to another.')
    parse.add_argument('--width', '-w', type=int, default=2, help='Set the width of the view option.')
    parse.add_argument('--path', '-p', default='accounts', help='Specify an alternative path to the service accounts.')
    parse.add_argument('--threads', type=int, default=None,help='Specify a different thread count. Cannot be greater than the amount of service accounts available.')
    parse.add_argument('--skip-bad-dests',default=False,action='store_true',help='Skip any destionations that cannot be read.')
    parsereq = parse.add_argument_group('required arguments')
    parsereq.add_argument('--source-id', '-s',help='The source ID of the folder to copy.',required=True)
    parsereq.add_argument('--destination-id', '-d',action='append',help='The destination ID of the folder to copy to.',required=True)
    args = parse.parse_args()
    mfc = multifolderclone(
        source=args.source_id,
        dest=args.destination_id,
        path=args.path,
        width=args.width,
        thread_count=args.threads,
        skip_bad_dests=args.skip_bad_dests
    )
    try:
        mfc.clone()
    except ValueError as e:
        print(e)
