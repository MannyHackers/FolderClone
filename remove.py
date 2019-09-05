from google.oauth2.service_account import Credentials
import googleapiclient.discovery, glob, sys, time, argparse

controls = glob.glob('controller/*.json')

parse = argparse.ArgumentParser(description='A tool to remove users from a shared drive.')
oft = parse.add_mutually_exclusive_group(required=True)
oft.add_argument('--prefix',help='Remove users that match a prefix.')
oft.add_argument('--suffix',help='Remove users that match a suffix.')
oft.add_argument('--role',help='Remove users based on permission roles.')
parsereq = parse.add_argument_group('required arguments')
parsereq.add_argument('--drive-id','-d',help='The ID of the Shared Drive.',required=True)
args = parse.parse_args()

valid_roles = ['owner','organizer','fileorganizer','writer','reader','commenter']
valid_levels = ['owner','manager','content manager','contributor','viewer','commenter']
if args.role:
	if args.role.lower() in valid_levels:
		role = valid_roles[valid_levels.index(args.role.lower())]
	elif args.role.lower() in valid_roles:
		role = args.role.lower()
	else:
		print('Invalid role.')
		sys.exit(0)

try:
	key = controls[0]
except IndexError:
	print('No controller found.')
	sys.exit(0)

credentials = Credentials.from_service_account_file(key, scopes=[
	"https://www.googleapis.com/auth/drive"
])

drive = googleapiclient.discovery.build("drive", "v3", credentials=credentials)

print('Getting permissions...')

rp = drive.permissions().list(fileId=args.drive_id,fields='permissions(id,emailAddress,role)',supportsAllDrives=True).execute()
cont = True
all_perms = []
while cont:
	all_perms += rp['permissions']
	if "nextPageToken" in rp:
		rp = drive.permissions().list(fileId=args.drive_id,supportsAllDrives=True,pageToken=rp["nextPageToken"]).execute()
	else:
		cont = False
batch = drive.new_batch_http_request()
tbr = 0
for i in all_perms:
	if args.prefix:
		if i['emailAddress'].split('@')[0].startswith(args.prefix):
			tbr += 1
			batch.add(drive.permissions().delete(fileId=args.drive_id,permissionId=i['id'],supportsAllDrives=True))
	elif args.suffix:
		if i['emailAddress'].split('@')[0].endswith(args.suffix):
			tbr += 1
			batch.add(drive.permissions().delete(fileId=args.drive_id,permissionId=i['id'],supportsAllDrives=True))
	elif args.role:
		if role == i['role'].lower():
			tbr += 1
			batch.add(drive.permissions().delete(fileId=args.drive_id,permissionId=i['id'],supportsAllDrives=True))
try:
	resp = input('Remove %d users. Ctrl-C to cancel, Enter to continue.' % tbr)
except KeyboardInterrupt:
	sys.exit(0)
print('Removing users.')
batch.execute()
print('Users removed.')