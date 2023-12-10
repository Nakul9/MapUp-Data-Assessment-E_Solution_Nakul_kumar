

import os
from dotenv import load_dotenv
import pandas as pd
import requests
import pyarrow.parquet as pq
from datetime import timedelta
import argparse
import json

# Load environment variables from .env file
load_dotenv()

# Access environment variables
api_key = os.getenv("TOLLGURU_API_KEY")
api_url = os.getenv("TOLLGURU_API_URL")






# Create an ArgumentParser object
parser = argparse.ArgumentParser(description='Process GPS data from Parquet file.')
parser.add_argument('--to_process', type=str, help='Path to the Parquet file to be processed.')
parser.add_argument('--output_dir', type=str, help='The folder to store the resulting Json files.')

# Parse the command-line arguments
args = parser.parse_args()

# Access the argument values
to_process_path = args.to_process
output_dir = args.output_dir
# Add the command-line arguments
url = api_url
folder_path =  to_process_path
files = os.listdir(folder_path)
headers = {'x-api-key': api_key, 'Content-Type': 'text/csv'}
for file_path in files:
    with open(folder_path+'/'+file_path, 'rb') as file:
        print(file_path)
        response = requests.post(url, data=file, headers=headers)
        response_data=json.loads(response.text)
#         print(output_dir+'/'+file_path)
        file_path=file_path.replace('.csv','.json')
        with open(output_dir+'/'+file_path, 'w') as json_file:
    # Serialize the Python object and write it to the file
            json.dump(response_data, json_file, indent=2)
    
        

    

