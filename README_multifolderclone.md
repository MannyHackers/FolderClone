Steps on how to use `multifolderclone.py`
=================================

Steps to setup `multifactory.py`
---------------------------------
1) Head over to <https://console.developers.google.com/> and sign in with your account.
2) Click "Library" on the left column, then click on "Select a project" at the top. Click on `NEW PROJECT` on the top-right corner of the new window.
3) In the Project name section, input a project name of your choice. Wait till the project creation is done and then click on "Select a project" again at the top and select your project.
4) Select "OAuth consent screen" and fill out the **Application name** field with a name of your choice. Scroll down and hit "Save"
5) Select "Credentials"  and select Create credentials. Choose "OAuth client ID". Choose "Other" as your **Application type** and hit "Create". Hit "Ok". You will now be presented with a list of "OAuth 2.0 client IDs". At the right end, there will be a download icon. Select it to download and save it as `credentials.json` in the script folder.
6) Find out how many projects you'll need. For example, a 100 TB job will take approximately 135 service accounts to make a full clone. Each project can have a maximum of 100 service accounts. In the case of the 100TB job, we will need 2 projects. `multifactory.py` conveniently includes a quick setup option. Run the following command `python3 multifactory.py --quick-setup N`. **Replace `N` with the amount of projects you need!**. If you want to only use new projects instead of existing ones, make sure to add `--new-only` flag. It will automatically start doing all the hard work for you.
6a) Running this for the first time will prompt you to login with your Google account. Login with the same account you used for Step 1. If will then ask you to enable a service. Open the URL in your browser to enable it. Press Enter once it's enabled.

Steps to add all the service accounts to the Shared Drive
---------------------------------
1) Once `multifactory.py` is done making all the accounts, open Google Drive and make a new Shared Drive to copy to.
2) Run the following command `python3 masshare.py -d SDFolderID`. Replace the `SDFolderID` with `XXXXXXXXXXXXXXXXXXX`. The Folder ID can be obtained from the Shared Drive URL `https://drive.google.com/drive/folders/XXXXXXXXXXXXXXXXXXX`. `masshare.py` will start adding all your service accounts.

**Shared Drives can only fit up to 600 users!**

Steps to clone a public folder to the Shared Drive
---------------------------------
1) Run the following command, `python3 multifolderclone.py -s SourceFolderID -d SDFolderID`. Replace `SourceFolderID` with the folder ID of the folder you are trying to copy and replace `SDFolderID` with the same ID as used in step 2 in `Steps to add service accounts to a Shared Drive`. It will start cloning the folder into the Shared Drive.

Steps to *sync* a public folder to the Shared Drive
---------------------------------
`multifolderclone.py` will now know if something's been copied already! Run the command again to copy over any new or missing files. *`multifolderclone.py` will not delete any files in the destination **not** in the source*

### As always, use the [Issues](https://github.com/Spazzlo/folderclone/issues) tab for any bugs, issues, feature requests or documentation improvements.
