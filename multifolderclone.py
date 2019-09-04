from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import time, glob, sys, argparse, httplib2shim, threading, progressbar

threads = None
pbar = None
httplib2shim.patch()
jobs = []

def _ls_(parent, searchTerms, drive):
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

def _lsd_(parent, drive):
    
    return _ls_(parent, " and mimeType contains 'application/vnd.google-apps.folder'", drive)

def _lsf_(parent, drive):
    
    return _ls_(parent, " and not mimeType contains 'application/vnd.google-apps.folder'", drive)

def _rebuild_dirs_(source, dest, drive):
    global jobs
    global pbar
    
    folderstocopy = _lsd_(source, drive)
    for i in folderstocopy:
        resp = drive.files().create(body={
            "name": i["name"],
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [dest]
        }, supportsAllDrives=True).execute()

        for file in _lsf_(source,drive):
            jobs.append({
                'source': file['id'],
                'destination': dest
            })
        pbar.update()
        _rebuild_dirs_(i["id"], resp["id"], drive)

def _copy_(drive, batch):
    global threads

    batch_copy = drive.new_batch_http_request()
    for job in batch:
        batch_copy.add(drive.files().copy(fileId=job['source'], body={"parents": [job['destination']]}, supportsAllDrives=True))
    batch_copy.execute()
    threads.release()

def _rcopy_(drives,batchsize,threadcount):
    global jobs
    global threads

    total_drives = len(drives)
    selected_drive = 0

    threads = threading.BoundedSemaphore(threadcount)

    print('Copying Files')
    pbar = progressbar.ProgressBar(max_value=len(jobs))
    final = [jobs[i * batchsize:(i + 1) * batchsize] for i in range((len(jobs) + batchsize - 1) // batchsize )]
    files_copied = 0
    for batch in final:
        threads.acquire()
        thread = threading.Thread(target=_copy_,args=(drives[selected_drive],batch))
        thread.start()
        files_copied += len(batch)
        pbar.update(files_copied)
        selected_drive += 1
        if selected_drive == total_drives - 1:
            selected_drive = 0
    pbar.finish()

def multifolderclone(source,dest,view='tree',width=2,path='accounts',batchsize=100,threadcount=50):

    accounts = glob.glob(path + '/*.json')

    check = build("drive", "v3", credentials=Credentials.from_service_account_file(accounts[0]))
    try:
        root_dir = check.files().get(fileId=source, supportsAllDrives=True).execute()['name']
    except HttpError:
        print('Source folder cannot be read or is invalid.')
        sys.exit(0)
    try:
        check.files().get(fileId=dest, supportsAllDrives=True).execute()
    except HttpError:
        print('Destination folder cannot be read or is invalid.')
        sys.exit(0)

    drives = []
    # pbar = ProgressBar("Ceating Drive Services", max=len(accounts))
    print('Creating Drive Services')
    for account in progressbar.progressbar(accounts):
        credentials = Credentials.from_service_account_file(account, scopes=[
            "https://www.googleapis.com/auth/drive"
        ])
        drives.append(build("drive", "v3", credentials=credentials))
    # pbar.finish()

    print('Rebuilding Folder Hierarchy')
    global pbar
    pbar = progressbar.ProgressBar(widgets=[progressbar.Timer()]).start()
    _rebuild_dirs_(source, dest, drives[0])
    pbar.finish()
    
    print('Copying files.')
    _rcopy_(drives,batchsize,threadcount)

if __name__ == '__main__':
    parse = argparse.ArgumentParser(description='A tool intended to copy large files from one folder to another.')
    parse.add_argument('--path','-p',default='accounts',help='Specify an alternative path to the service accounts.')
    parse.add_argument('--threads',default=50,help='Specify the amount of threads to use.')
    parse.add_argument('--batch-size',default=100,help='Specify how large the batch requests should be.')
    parsereq = parse.add_argument_group('required arguments')
    parsereq.add_argument('--source-id','-s',help='The source ID of the folder to copy.',required=True)
    parsereq.add_argument('--destination-id','-d',help='The destination ID of the folder to copy to.',required=True)
    args = parse.parse_args()

    print('Copy from %s to %s.' % (args.source_id,args.destination_id))

    multifolderclone(
        args.source_id,
        args.destination_id,
        args.view,
        args.width,
        args.path,
        args.threads,
        args.batch_size
    )
