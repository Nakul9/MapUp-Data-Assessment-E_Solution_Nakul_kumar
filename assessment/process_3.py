
import os
from dotenv import load_dotenv
import pandas as pd
import requests
import pyarrow.parquet as pq
from datetime import timedelta
import argparse
import json
import warnings
warnings.filterwarnings("ignore")


# Create an ArgumentParser object
parser = argparse.ArgumentParser(description='Process GPS data from Parquet file.')
parser.add_argument('--to_process', type=str, help='Path to the Parquet file to be processed.')
parser.add_argument('--output_dir', type=str, help='The folder to store the resulting Json files.')

# Parse the command-line arguments
args = parser.parse_args()

# Access the argument values
to_process_path = args.to_process
output_dir = args.output_dir

print(to_process_path,output_dir)

files = os.listdir(to_process_path)
final=pd.DataFrame()
for file_path in files:
    with open(os.path.join(to_process_path, file_path), 'r') as file:
        
        data = json.load(file)
        
        def extract_toll_info(toll):
            try:
                
                toll_info = {

                    'toll_loc_id_start': toll['start'].get('id'),


                    'toll_loc_id_end': toll['end'].get('id'),
                    'toll_loc_name_start': toll['start'].get('name'),
                    'toll_loc_name_end': toll['end'].get('name'),
                    'toll_system_type': toll.get('type'),
                    'entry_time': toll['start'].get('timestamp_formatted'),
                    'exit_time': toll['end'].get('timestamp_formatted'),
                    'tag_cost': toll.get('tagCost'),
                    'cash_cost': toll.get('cashCost'),
                    'license_plate_cost': toll.get('licensePlateCost'),
                }
            except KeyError as e:
                
                toll_info = {
                    'toll_loc_id_start': None,
                    'toll_loc_id_end': None,
                    'toll_loc_name_start': None,
                    'toll_loc_name_end': None,
                    'toll_system_type': None,
                    'entry_time': None,
                    'exit_time': None,
                    'tag_cost': None,
                    'cash_cost': None,
                    'license_plate_cost': None,
        }
            return toll_info

        toll_info_list = [extract_toll_info(toll) for toll in data['route'].get('tolls', [])]

        # Convert the list of dictionaries to a pandas DataFrame
        df = pd.DataFrame(toll_info_list)

        # Add 'uuid' to the DataFrame
        df['unit'] = file_path[:4]
        df['trip_id'] = file_path.replace('.json', '')

        # Replace None with NaN (null values) in the DataFrame
        df = df.where(pd.notna(df), None)

        # Concatenate DataFrames
        final = pd.concat([final, df], ignore_index=True)



# Drop duplicate rows in the final DataFrame
final.drop_duplicates(inplace=True)
final=final[['unit', 'trip_id','toll_loc_id_start', 'toll_loc_id_end', 'toll_loc_name_start',
       'toll_loc_name_end', 'toll_system_type', 'entry_time', 'exit_time',
       'tag_cost', 'cash_cost', 'license_plate_cost']]
final.to_csv(output_dir+'/'+'transformed_data.csv',index=False)
