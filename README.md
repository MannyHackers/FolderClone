# folderclone - A project that allows you copy large folders to Shared Drives.


## Installation

folderclone is available on PyPI, so you can install it using pip.

    pip install folderclone


## multimanager

Multi Manager is the tool that will help you setup everything you need to make folderclone work.

### Multi Manager Setup
To set it up, head over to the [Python Quickstart](https://developers.google.com/drive/api/v3/quickstart/python) page and click the Enable the Drive API. Go through the setup and once its done, download the credentials to a new folder on your computer.

On your terminal, change your directory to that folder you just made and run:

    multimanager interactive

This will start multimanager in interactive mode. It'll start by taking you to a login page to authenticate yourself. You'll then be met with a prompt to enable the Service Usage API. Visit the link it provides, enable the API, then go back and press Enter to retry. Don't worry about having to do this every time, this is a one time setup.

Once it's done, you'll be met with the Multi Manager prompt.

    Multi Manager
    mm>
You have successfully setup Multi Manager!

#### Quick Setup

For folderclone, you'll need a few Service Accounts (SAs) ready. To do this in, run:

    mm> quick-setup N SHARED_DRIVE_ID

Where `N` is the amount of projects you'd like to use and `SHARED_DRIVE_ID` the ID of the Shared Drive you'd like to copy to.

For example, say I wanted to copy 100 TB worth of content. I'd need 134 SAs (750 GB each) to do the copy, so 2 projects. I'll be copying to a fresh new Shared Drive who's ID is 0ABCdeyz_ZaMsxxxLGA. I'll be running:

    mm> quick-setup 2 0ABCdeyz_ZaMsxxxLGA

This will automatically;
- create 2 projects
- enable the required services
- create Service Accounts
- add them to the Shared Drive
- and download their credentials into a new folder `accounts`

You are now ready to go to the next step.

## multifolderclone

multifoldeclone is the tool that will do all the cloning for you. It is the simplest thing to use.

    multifolderclone -s SOURCE_FOLDER_ID -d DESTINATION_FOLDER_ID

Where `SOURCE_FOLDER_ID` is the ID of the folder you'll want to copy (Make sure the source folder is accessible to the service accounts by either making the folder public or sharing the folder with the service accounts you are using to copy with), and `DESTINATION_FOLDER_ID` is the ID of the folder you are copying to. This could be the ID of the Shared Drive, or a folder inside the Shared Drive.

This will automatically start cloning the folder!
And that's it! You did it!
