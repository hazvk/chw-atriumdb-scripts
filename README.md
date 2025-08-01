# chw-atriumdb-scripts

This repository contains Python scripts for using the AtriumDB database at CHW.

## Scripts

> Assume the scripts are stored in your `BDSP/` folder on the server.

### [`login_to_atrium_sdk.py`](login_to_atrium_sdk.py)
- **Purpose:** Sets up a connection to the AtriumDB using the `AtriumSDK`.
- **Usage:** Import the `local_prod_sdk` object in your scripts to access the database.
- **Note:** You must fill in your username and password in the `local_prod_connection_params` object before use.

### [`pat_measure_extract.py`](pat_measure_extract.py)
- **Purpose:** Extracts patient measures for a given MRN and specified measure tags, merges them, and saves the results to CSV files. The output is zipped and the original folder is cleaned up.


> IMPORTANT: once you have SFTPed the zip file into the network drive, please delete from the server to avoid memory issues on the server down the track.

- **Usage:**  
  ```sh
  python pat_measure_extract.py <MRN> <measures_required_like>
  ```
  Example:
  ```sh
  python pat_measure_extract.py 1689501 "rSO,ART"
  ```
- **Output:**  
  - Zipped CSV files in `_data/pat_measure_extract/<MRN>.<timestamp>.zip`
  - Log files for extraction details.
- **Notes:**
  - The script splits large datasets into multiple CSV files to avoid size issues.
  - All extracted files are automatically zipped and the original folder is deleted to save space.
  - Log files are created both in the working directory and the extraction folder for traceability.
  - File paths in logs are relative to the BDSP folder.
  - When extracting, timestamps are based on nanoseconds since epoch and converted to readable format in filenames.

## Requirements

- Python 3.x
- pandas
- The `atriumdb` Python package (must be available in your environment)

## Additional Information

- Ensure your credentials are set in [`login_to_atrium_sdk.py`](login_to_atrium_sdk.py).
- For more details, see comments in each script.
- After extraction, use SFTP to download the zipped data to your local machine as described in the script logs.

---
For further guidance, refer to the [BDSP Data Export Tutorial](https://nswhealth.sharepoint.com/:w:/r/sites/BigDataforSmallPeople-SCHN/Shared%20Documents/General/BDSP%20Data%20Export%20Tutorial.docx?d=w42728ee5f3f24b3cb726eae94d51a858&csf=1&web=1&e=4TSqrN&nav=eyJoIjoiMzgxNjE4OTU2In0).
