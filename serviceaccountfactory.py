from google.oauth2.service_account import Credentials
import googleapiclient.discovery, base64, json, progress.bar, glob, sys, argparse, time
from os import mkdir

stt = time.time()

parse = argparse.ArgumentParser(description='A tool to create Google service accounts.')
parse.add_argument('--path','-p',default='accounts',help='Specify an alternate directory to output the credential files.')
parse.add_argument('--controller','-c',default='controller/*.json',help='Specify the relative path for the controller file.')
parse.add_argument('--no-autofill',default=False,action='store_true',help='Do not autofill the first project.')

args = parse.parse_args()
acc_dir = args.path
contrs = glob.glob(args.controller)

def create_service_account_and_dump_key(project_id, service_account_name, service_account_filename):
	
	service_account = iam.projects().serviceAccounts().create(
		name="projects/" + project_id,
		body={
			"accountId": service_account_name,
			"serviceAccount": {
				"displayName": service_account_name
			}
		}
	).execute()

	key = iam.projects().serviceAccounts().keys().create(
		name="projects/" + project_id + "/serviceAccounts/" + service_account["uniqueId"],
		body={
			"privateKeyType": "TYPE_GOOGLE_CREDENTIALS_FILE",
			"keyAlgorithm": "KEY_ALG_RSA_2048"
		}
	).execute()
	
	f = open(service_account_filename, "w")
	f.write(base64.b64decode(key["privateKeyData"]).decode("utf-8"))
	f.close()

try:
	open(contrs[0],'r')
	print('Found controllers.')
except IndexError:
	print('No controller found.')
	sys.exit(0)

proj = 0
pid = 'pid'
projects = {}
print('Add more projects:')
print('[project id] [accounts to create]')
if not args.no_autofill:
	proj += 1
	pid = json.loads(open(contrs[0],'r').read())['project_id']
	projects[pid] = 99
	print(str(proj) + '. ' + pid + ' 99')

while pid != '':
	proj += 1
	pid = input(str(proj) + '. ')
	if pid:
		a = pid.split()
		projects[a[0]] = a[1]

prefix = ''

while len(prefix) < 4:
	prefix = input('Custom email prefix? ').lower()
	if prefix == '':
		prefix = 'folderclone'
	if len(prefix) < 4:
		print('Email prefix must be 5 characters or longer!')

print('Using ' + str(len(projects)) + ' projects...')

credentials = Credentials.from_service_account_file(contrs[0], scopes=[
	"https://www.googleapis.com/auth/iam"
	])
iam = googleapiclient.discovery.build("iam", "v1", credentials=credentials)

try:
	mkdir(acc_dir)
except FileExistsError:
	pass

gc = 1
for i in projects:
	pbar = progress.bar.Bar("Creating accounts in %s" % i,max=int(projects[i]))
	for o in range(1, int(projects[i]) + 1):
		create_service_account_and_dump_key(i,prefix + str(o),acc_dir + "/" + str(gc) + '.json')
		gc += 1
		pbar.next()
	pbar.finish()

print('Complete.')
hours, rem = divmod((time.time() - stt),3600)
minutes, sec = divmod(rem,60)
print("Elapsed Time:\n{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),sec))
