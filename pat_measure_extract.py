# Usage: python pat_measure_extract.py <MRN> <measures_required_like>
# Example: python pat_measure_extract.py 1689501 "rSO,ART"

####### ABOUT THIS SCRIPT #######
# This script extracts patient measures for a given MRN and specified measures,
# merging them into a single DataFrame, and saving the results to multiple CSV files.
# We use multiple CSV files to avoid the file being too large to be used.
# It then zips the folder containing the CSV files, and cleans up the original folder.

# NOTE: the file paths in the logs will be relative to the BDSP folder
# NOTE: the zip file will save to a location named like:
# BDSP/_data/pat_measure_extract/<MRN>.<time_of_extraction>.zip
# NOTE: when you extract the zip, the files in it will be named like:
# pat_measure_extract.<MRN>.<encounter_time_min>-<encounter_time_max>.csv
# That way, you can use only the data for the time frame that is relevant to you.
# NOTE: ensure you SFTP the zip file to your local machine once done
###############################


import datetime
import sys
import pandas as pd
import os
import logging

# I have created a script in the same directory that creates the local_prod_sdk object
# You can remove the below line and copy in however you define local_prod_sdk
from login_to_atrium_sdk import local_prod_sdk

module_name = os.path.basename(__file__).replace('.py', '')

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__file__)
logger.addHandler(logging.FileHandler(f'{module_name}.log', mode='w'))
# Ensure the script is run with the correct number of arguments
if len(sys.argv) < 3:
    raise Exception(f'Usage: python {module_name}.py <mrn> <measures_required_like>')

mrn = sys.argv[1] # e.g. '1689501'
if not mrn:
    raise Exception('No MRN provided.')

measures_required_like = sys.argv[2].split(',') # e.g. 'rSO,ART'
if not measures_required_like:
    raise Exception('No measures provided.')

folder_path = f'_data/{module_name}/{mrn}.{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
os.makedirs(folder_path, exist_ok=True)
logger.info(f"Folder created at {folder_path} for storing extracted data.")
logger.addHandler(logging.FileHandler(f'{folder_path}/{module_name}.log', mode='w'))

all_measures = pd.DataFrame(local_prod_sdk.get_all_measures().values())
relevant_measures = []
for mt_idx, measure_tag in enumerate(measures_required_like):
    measure_tag = measure_tag.strip()
    if measure_tag:
        relevant_measures.extend(all_measures[[measure_tag in (m or '') for m in all_measures['tag']]][['id','tag']].values.tolist())
    else:
        logging.warning(f"Empty measure tag provided, skipping: {measure_tag} at index {mt_idx}")

if len(relevant_measures) == 0:
    raise Exception('No measures found for the given tags.')

all_patient_measure_data = []

logger.info(f"Extracting data for MRN {mrn} with measures: {', '.join([measure[1] for measure in relevant_measures])}")
for measure in relevant_measures:
    data = local_prod_sdk.get_data(mrn=int(mrn), start_time_n=0, end_time_n=1743043334 * 10**9, measure_id=measure[0])
    data_df = pd.DataFrame({'mrn': mrn, 'epoch':data[1], f'{measure[1]}_epoch':data[1], f'{measure[1]}_value':data[2]})
    data_df['epoch'] = data_df['epoch'].astype('float64')
    all_patient_measure_data.append(data_df)

JOIN_GAP = 1024000000
mega_df = pd.DataFrame()
for df in all_patient_measure_data:
    if mega_df.empty:
        mega_df = df
    else:
        mega_df = pd.merge_asof(mega_df, df, on='epoch', by='mrn', direction='nearest', tolerance=JOIN_GAP)

mega_df.sort_values(by=['mrn', 'epoch'], inplace=True)

logger.info(f"Data for MRN {mrn} extracted. Total rows: {len(mega_df)}. Saving to {folder_path}")
ROW_EXTRACT_SIZE = 100000
for row_idx in range(0, len(mega_df), ROW_EXTRACT_SIZE):
    row_end = min(row_idx + ROW_EXTRACT_SIZE, len(mega_df))
    logger.info(f"- Extracting rows {row_idx} to {row_end} of {len(mega_df)}")
    min_time = pd.to_datetime(mega_df['epoch'].iloc[row_idx], unit='ns').strftime('%Y%m%d%H%M%s')
    max_time = pd.to_datetime(mega_df['epoch'].iloc[row_end - 1], unit='ns').strftime('%Y%m%d%H%M%s')
    file_name = f'{folder_path}/pat_measure_extract.{mrn}.{min_time}-{max_time}.csv'
    
    mega_df.to_csv(file_name, index=False)
    logger.info(f"-- Data saved to {file_name}")

logger.info(f"Extraction complete. Total rows extracted: {len(mega_df)}")

# Zip the folder
import shutil
shutil.make_archive(folder_path, 'zip', folder_path)
logger.info(f"Folder zipped to {folder_path}.zip")

# Clean up the original folder
shutil.rmtree(folder_path)

logger.info(f"Now log back onto Windows machine, log into SFTP shell and run `get BDSP/{folder_path}.zip` to download the file.")
logger.info(f"For more info, see https://nswhealth.sharepoint.com/:w:/r/sites/BigDataforSmallPeople-SCHN/Shared%20Documents/General/BDSP%20Data%20Export%20Tutorial.docx?d=w42728ee5f3f24b3cb726eae94d51a858&csf=1&web=1&e=4TSqrN&nav=eyJoIjoiMzgxNjE4OTU2In0")

logger.info("Done.")