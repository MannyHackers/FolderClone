from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from itertools import islice
import time, glob, sys, argparse, httplib2shim, threading, progressbar

threads = None
pbar = None
errd = {}
jobs = {}
httplib2shim.patch()

def _chunks(job_dict,size):
    it = iter(job_dict)
    for i in range(0,len(job_dict),size):
        yield {k:job_dict[k] for k in islice(it,size)}

def _ls(parent, searchTerms, drive):
    while True:
        try:
            files = []
            resp = drive.files().list(q="'%s' in parents" % parent + searchTerms, pageSize=1000, supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            files += resp["files"]
            while "nextPageToken" in resp:
                resp = drive.files().list(q="'%s' in parents" % parent + searchTerms, pageSize=1000, supportsAllDrives=True, includeItemsFromAllDrives=True, pageToken=resp["nextPageToken"]).execute()
                files += resp["files"]
            return files
        except Exception as e:
            time.sleep(3)

def _lsd(parent, drive):
    
    return _ls(parent, " and mimeType contains 'application/vnd.google-apps.folder'", drive)

def _lsf(parent, drive):
    
    return _ls(parent, " and not mimeType contains 'application/vnd.google-apps.folder'", drive)

def _rebuild_dirs(source, dest, drive):
    global jobs
    global pbar
    
    for file in _lsf(source,drive):
        jobs[file['id']] = dest
        pbar.update()
        
    folderstocopy = _lsd(source, drive)
    for i in folderstocopy:
        resp = drive.files().create(body={
            "name": i["name"],
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [dest]
        }, supportsAllDrives=True).execute()
        _rebuild_dirs(i["id"], resp["id"], drive)

def _batch_response(id,resp,exception):
    global errd
    global jobs
    if exception is not None:
        fileId = str(exception).split('/')[6]
        errd[fileId] = jobs[fileId]

def _copy(drive, batch):
    global threads

    batch_copy = drive.new_batch_http_request()
    for job in batch:
        batch_copy.add(drive.files().copy(fileId=job, body={"parents": [batch[job]]}, supportsAllDrives=True), callback=_batch_response)
    batch_copy.execute()
    threads.release()

def _rcopy(drives,batch_size,thread_count):
    global jobs
    global threads

    total_drives = len(drives)
    selected_drive = 0

    final = []
    for i in _chunks(jobs,batch_size):
        final.append(i)

    threads = threading.BoundedSemaphore(thread_count)

    pbar = progressbar.ProgressBar(max_value=len(jobs))
    files_copied = 0
    for batch in final:
        threads.acquire()
        thread = threading.Thread(target=_copy,args=(drives[selected_drive],batch))
        thread.start()
        files_copied += len(batch)
        pbar.update(files_copied)
        selected_drive += 1
        if selected_drive == total_drives - 1:
            selected_drive = 0
    while threading.active_count() != 1:
        time.sleep(1)
    pbar.finish()

def multifolderclone(source=None,dest=None,path='accounts',batch_size=100,thread_count=50):
    global jobs
    global errd

    print(threading.active_count())
    accounts = glob.glob(path + '/*.json')

    check = build("drive", "v3", credentials=Credentials.from_service_account_file(accounts[0]))
    try:
        root_dir = check.files().get(fileId=source, supportsAllDrives=True).execute()['name']
    except HttpError:
        print('Source folder cannot be read or is invalid.')
        sys.exit(0)
    try:
        dest_dir = check.files().get(fileId=dest, supportsAllDrives=True).execute()['name']
    except HttpError:
        print('Destination folder cannot be read or is invalid.')
        sys.exit(0)

    drives = []
    print('Creating Drive Services')
    for account in progressbar.progressbar(accounts):
        credentials = Credentials.from_service_account_file(account, scopes=[
            "https://www.googleapis.com/auth/drive"
        ])
        drives.append(build("drive", "v3", credentials=credentials))

    print('Rebuilding Folder Hierarchy for %s in %s' % (root_dir,dest_dir))
    global pbar
    pbar = progressbar.ProgressBar(widgets=[progressbar.Timer()]).start()
    _rebuild_dirs(source, dest, drives[0])
    pbar.finish()
    
    print('Copying files from %s to %s' % (root_dir,dest_dir))
    _rcopy(drives,batch_size,thread_count)
    while len(errd) > 0:
        print('Dropped %d files...\nRetrying' % len(errd))
        jobs = errd
        errd = {}
        _rcopy(drives,batch_size,thread_count)

if __name__ == '__main__':
    parse = argparse.ArgumentParser(description='A tool intended to copy large files from one folder to another.')
    parse.add_argument('--path','-p',default='accounts',help='Specify an alternative path to the service accounts.')
    parse.add_argument('--threads',default=50,help='Specify the amount of threads to use. USE AT YOUR OWN RISK.')
    parse.add_argument('--batch-size',default=100,help='Specify how large the batch requests should be. USE AT YOUR OWN RISK.')
    parsereq = parse.add_argument_group('required arguments')
    parsereq.add_argument('--source-id','-s',help='The source ID of the folder to copy.',required=True)
    parsereq.add_argument('--destination-id','-d',help='The destination ID of the folder to copy to.',required=True)
    args = parse.parse_args()

    print('Copy from %s to %s.' % (args.source_id,args.destination_id))

    multifolderclone(
        args.source_id,
        args.destination_id,
        args.path,
        args.batch_size,
        args.threads
    )
