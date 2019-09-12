from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from urllib3.exceptions import ProtocolError
import googleapiclient.discovery, progress.bar, time, threading, httplib2shim, glob, sys, argparse, json, socket

drive = []
accounts = None
dtu = None
width = None
threads = None

class TransferRateLimit(Exception):
    pass

def apicall(request):
    resp = _apicall(request)
    while not resp:
        resp = _apicall(request)
    return resp

def _apicall(request):
    try:
        return request.execute()
    except HttpError as error:
        details = json.loads(error.content.decode("utf-8"))
        code = details["error"]["code"]
        reason = details["error"]["errors"][0]["reason"]
        message = details["error"]["errors"][0]["message"]
        if code in [400, 401, 404]:
            print("gapi error code " + str(code) + " [" + reason + "]: " + message)
            raise error
            sys.exit()
        elif code in [429, 500, 503]:
            return False
        elif code == 403:
            if reason in ["dailyLimitExceeded", "rateLimitExceeded"]:
                return False
            if reason == "userRateLimitExceeded":
                raise TransferRateLimit()
                return True
            elif reason in ["sharingRateLimitExceeded", "appNotAuthorizedToFile", "insufficientFilePermissions", "domainPolicy"]:
                print("gapi error code " + str(code) + " [" + reason + "]: " + message)
                raise error
                sys.exit()
            else:
                print("unknown reason '" + reason + "'")
                print("Exiting script...")
                sys.exit()
        else:
            print("unknown error code " + str(code))
            raise error
    except socket.error as error:
        return False
    except ProtocolError as error:
        return False

def ls(parent, searchTerms=""):
    files = []
    resp = apicall(drive[0].files().list(q=f"'{parent}' in parents" + searchTerms, pageSize=1000, supportsAllDrives=True, includeItemsFromAllDrives=True))
    files += resp["files"]
    
    while "nextPageToken" in resp:
        resp = apicall(drive[0].files().list(q=f"'{parent}' in parents" + searchTerms, pageSize=1000, supportsAllDrives=True, includeItemsFromAllDrives=True, pageToken=resp["nextPageToken"]))
        files += resp["files"]
    return files

def lsd(parent):
    return ls(parent, searchTerms=" and mimeType contains 'application/vnd.google-apps.folder'")

def lsf(parent):
    return ls(parent, searchTerms=" and not mimeType contains 'application/vnd.google-apps.folder'")

def copy(source, dest):
    global drive
    global dtu
    global accounts
    global threads
    
    cached_dtu = dtu

    while True:
        try:
            apicall(drive[cached_dtu].files().copy(fileId=source, body={"parents": [dest]}, supportsAllDrives=True))
        except TransferRateLimit as error:
            if accounts == cached_dtu:
                drive.pop(cached_dtu)
                accounts -= 1
                dtu = 1
                cached_dtu = dtu
            elif accounts != 1:
                # Removing Quotad Account
                drive.pop(cached_dtu)
                accounts -= 1
            else:
                print("No more accounts available to clone files with. Exitting script...")
                sys.exit()
        else:
            break
    threads.release()

def rcopy(source, dest, sname,pre):
    global drive
    global accounts
    global dtu
    global width
    global threads

    pres = pre

    filestocopy = lsf(source)
    if len(filestocopy) > 0:
        pbar = progress.bar.Bar(pres + sname, max=len(filestocopy))
        pbar.update()
        for i in filestocopy:
            threads.acquire()
            thread = threading.Thread(target=copy,args=(i["id"],dest))
            thread.start()
            dtu += 1
            if dtu == accounts:
                dtu = 1
            pbar.next()
        
        pbar.finish()
    else:
        print(pres + sname)
    
    folderstocopy = lsd(source)
    fs = len(folderstocopy) - 1
    s = 0
    for i in folderstocopy:
        if s == fs:
            nstu = pre.replace("├" + "─" * width + " ","│" + " " * width + " ").replace("└" + "─" * width + " ","  " + " " * width) + "└" + "─" * width + " "
        else:
            nstu = pre.replace("├" + "─" * width + " ","│" + " " * width + " ").replace("└" + "─" * width + " ","  " + " " * width) + "├" + "─" * width + " "
        resp = apicall(drive[0].files().create(body={
            "name": i["name"],
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [dest]
        }, supportsAllDrives=True))
        rcopy(i["id"], resp["id"], i["name"].replace('%',"%%"),nstu)
        s += 1

def main():
    global accounts
    global dtu
    global width
    global drive
    global threads

    stt = time.time()
    
    parse = argparse.ArgumentParser(description='A tool intended to copy large files from one folder to another.')
    parse.add_argument('--width','-w',default=2,help='Set the width of the view option.')
    parse.add_argument('--path','-p',default='accounts',help='Specify an alternative path to the service accounts.')
    parsereq = parse.add_argument_group('required arguments')
    parsereq.add_argument('--source-id','-s',help='The source ID of the folder to copy.',required=True)
    parsereq.add_argument('--destination-id','-d',help='The destination ID of the folder to copy to.',required=True)
    args = parse.parse_args()
    
    source_id = args.source_id
    dest_id = args.destination_id
    width = args.width
    
    print('Copy from %s to %s.' % (source_id,dest_id))
    print('View set to tree (%d).' % width)

    httplib2shim.patch()
    accounts = 0
    dtu = 1
    accsf = glob.glob(args.path + '/*.json')
    pbar = progress.bar.Bar("Creating Drive Services", max=len(accsf))
    for i in accsf:
        accounts += 1
        credentials = Credentials.from_service_account_file(i, scopes=[
            "https://www.googleapis.com/auth/drive"
        ])
        drive.append(googleapiclient.discovery.build("drive", "v3", credentials=credentials))
        pbar.next()
    pbar.finish()
    threads = threading.BoundedSemaphore(accounts)
    print('BoundedSemaphore with %d threads' % accounts)

    try:
        rcopy(source_id,dest_id , "root","")
    except KeyboardInterrupt:
        print('Quitting')
        sys.exit()
    except Exception as error:
        print(error)
        print("Error occured while copying. Exiting script...")
        sys.exit()
    print('Complete.')
    hours, rem = divmod((time.time() - stt),3600)
    minutes, sec = divmod(rem,60)
    print("Elapsed Time:\n{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),sec))
    sys.exit()

if __name__ == '__main__':
    main()