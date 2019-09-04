from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import progress.bar, time, glob, sys, argparse, httplib2shim, threading

threads = None
httplib2shim.patch()
jobs = []

def ls(parent, searchTerms, drive):
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

def lsd(parent, drive):
    
    return ls(parent, " and mimeType contains 'application/vnd.google-apps.folder'", drive)

def lsf(parent, drive):
    
    return ls(parent, " and not mimeType contains 'application/vnd.google-apps.folder'", drive)

# def copy(source, dest, selected_drive):
#     while True:
#         try:
#             copied_file = drives[selected_drive].files().copy(fileId=source, body={"parents": [dest]}, supportsAllDrives=True).execute()
#         except Exception as e:
#             time.sleep(3)
#         else:
#             break

def rebuild_dirs(source, dest, folder, pre, view, width, drive):
    global jobs

    pres = pre
    if view == 2:
        pres = ""
    elif view == 1:
        pres = " " * (int(((len(pre) - 4))/3) * width)

    print(pres + folder)
    
    folderstocopy = lsd(source, drive)
    fs = len(folderstocopy) - 1
    s = 0
    for i in folderstocopy:
        if s == fs:
            nstu = pre.replace("├" + "─" * width + " ","│" + " " * width + " ").replace("└" + "─" * width + " ","  " + " " * width) + "└" + "─" * width + " "
        else:
            nstu = pre.replace("├" + "─" * width + " ","│" + " " * width + " ").replace("└" + "─" * width + " ","  " + " " * width) + "├" + "─" * width + " "
        resp = drive.files().create(body={
            "name": i["name"],
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [dest]
        }, supportsAllDrives=True).execute()

        for file in lsf(source,drive):
            jobs.append({
                'source': file['id'],
                'destination': dest
            })

        rebuild_dirs(i["id"], resp["id"], i["name"].replace('%',"%%"), nstu, view, width, drive)
        s += 1

def rrcopy(drive, batch):
    global threads
    batch_copy = drive.new_batch_http_request()
    for job in batch:
        batch_copy.add(drive.files().copy(fileId=job['source'], body={"parents": [job['destination']]}, supportsAllDrives=True))
    batch_copy.execute()
    threads.release()

def rcopy(drives,batchsize,threadcount):
    global jobs
    global threads

    total_drives = len(drives)
    selected_drive = 0

    threads = threading.BoundedSemaphore(threadcount)

    final = [jobs[i * batchsize:(i + 1) * batchsize] for i in range((len(jobs) + batchsize - 1) // batchsize )]
    print('Drive set to ' + str(selected_drive))
    for batch in final:
        threads.acquire()
        thread = threading.Thread(target=rrcopy,args=(drives[selected_drive],batch))
        thread.start()
        selected_drive += 1
        if selected_drive == total_drives - 1:
            selected_drive = 0

def multifolderclone(source,dest,view='tree',width=2,path='accounts',batchsize=100,threadcount=50):

    accounts = glob.glob(path + '/*.json')

    check = build("drive", "v3", credentials=Credentials.from_service_account_file(accounts[0]))
    try:
        check.files().get(fileId=source, supportsAllDrives=True).execute()
    except HttpError:
        print('Source folder cannot be read or is invalid.')
        sys.exit(0)
    try:
        check.files().get(fileId=dest, supportsAllDrives=True).execute()
    except HttpError:
        print('Destination folder cannot be read or is invalid.')
        sys.exit(0)

    drives = []
    pbar = progress.bar.Bar("Ceating Drive Services", max=len(accounts))
    for account in accounts:
        credentials = Credentials.from_service_account_file(account, scopes=[
            "https://www.googleapis.com/auth/drive"
        ])
        drives.append(build("drive", "v3", credentials=credentials))
        pbar.next()
    pbar.finish()

    print('Rebuilding Folder Hierarchy')
    rebuild_dirs(source, dest, "root", "", view, width, drives[0])
    
    print('Copying files.')
    rcopy(drives,batchsize,threadcount)

if __name__ == '__main__':
    stt = time.time()

    parse = argparse.ArgumentParser(description='A tool intended to copy large files from one folder to another.')
    parse.add_argument('--view',default='tree',help='Set the view to a different setting (tree|indented|basic).')
    parse.add_argument('--width','-w',default=2,help='Set the width of the view option.')
    parse.add_argument('--path','-p',default='accounts',help='Specify an alternative path to the service accounts.')
    parse.add_argument('--threads',default=50,help='Specify the amount of threads to use.')
    parse.add_argument('--batch-size',default=100,help='Specify how large the batch requests should be.')
    parsereq = parse.add_argument_group('required arguments')
    parsereq.add_argument('--source-id','-s',help='The source ID of the folder to copy.',required=True)
    parsereq.add_argument('--destination-id','-d',help='The destination ID of the folder to copy to.',required=True)
    args = parse.parse_args()

    print('Copy from %s to %s.' % (args.source_id,args.destination_id))
    print('View set to %s (%d).' % (args.view,args.width))

    multifolderclone(
        args.source_id,
        args.destination_id,
        args.view,
        args.width,
        args.path,
        args.threads,
        args.batch_size
    )

    print('Complete.')
    hours, rem = divmod((time.time() - stt),3600)
    minutes, sec = divmod(rem,60)
    print("Elapsed Time:\n{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),sec))
