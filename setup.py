from sys import platform
from setuptools import setup,find_packages

install_requires = [
    'google_auth_oauthlib',
    'urllib3',
    'httplib2shim',
    'protobuf',
    'pyreadline',
    'google_api_python_client']

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(
     name='folderclone',
     entry_points={
        'console_scripts': [
            'multimanager=folderclonecli.mmparse:main',
            'multifolderclone=folderclonecli.mfcparse:main']
    },
     version='0.5.0',
     author='Spazzlo',
     description='A tool to copy large folders to Shared Drives.',
     long_description=long_description,
     long_description_content_type='text/markdown',
     url='https://github.com/Spazzlo/folderclone',
     packages=find_packages('src'),
     package_dir={'':'src'},
     install_requires=install_requires,
     classifiers=[
         'Programming Language :: Python :: 3',
         'License :: OSI Approved :: MIT License',
         'Operating System :: OS Independent',
     ]
 )