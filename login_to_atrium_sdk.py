# File: login_to_atrium_sdk.py

####### ABOUT THIS SCRIPT #######
# Store this in the BDSP folder
# For any Python files stored next to it, you can import this module 
# to get access to local_prod_sdk using the following code:
# from login_to_atrium_sdk import local_prod_sdk

# NOTE: you need to fill in username and password below before using
###############################

import sys
sys.path.extend(['./', '../'])
from atriumdb import AtriumSDK

local_prod_connection_params = {
    'user': "<<<YOUR_USERNAME_HERE>>>",
    'password': '<<<YOUR_PASSWORD_HERE>>>',
    'host': 'chwmana-als001.schn.health.nsw.gov.au',
    'port': 3306,
    'database': "atriumdb"
}

local_prod_dataset_location = "/apps/atriumdb/data"

local_prod_sdk = AtriumSDK(dataset_location=local_prod_dataset_location,
                           connection_params=local_prod_connection_params,
                           metadata_connection_type="mariadb")
