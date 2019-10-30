from argparse import ArgumentParser
from os.path import exists
from json import loads
from os import rename
from glob import glob

# args handler
def args_handler(mg,args):

    # default value for services if not set
    args.services = ['iam','drive'] if args.services is None else args.services

    # print any error
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
                if args.pattern.lower() in ('owner','organizer','fileorganizer','writer','reader','commenter'):
                    print('Removing accounts')
                    mg.remove_users(args.drive_id,role=args.pattern)
                else:
                    print('Invalid role %s. Choose from (owner,organizer,fileorganizer,writer,reader,commenter)' % args.pattern)

            # remove DRIVE_ID pattern SUFFIX
            elif args.pattern_type == 'suffix':
                print('Removing accounts')
                mg.remove_users(args.drive_id,suffix=args.pattern)

            # remove DRIVE_ID pattern ROLE
            elif args.pattern_type == 'prefix':
                print('Removing accounts')
                mg.remove_users(args.drive_id,prefix=args.pattern)
    except Exception as e:
        raise e

def main():
    from folderclone.multimanager import multimanager

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
    interact = subparsers.add_parser('interactive',help='Initiate Multi Manager in interactive mode.')

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
        if platform != 'win32':
            import readline
        else:
            import pyreadline
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
