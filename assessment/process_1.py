import pandas as pd
import requests
import pyarrow.parquet as pq
from datetime import timedelta


import argparse

# Create an ArgumentParser object
parser = argparse.ArgumentParser(description='Process GPS data from Parquet file.')

# Add the command-line arguments
parser.add_argument('--to_process', type=str, help='Path to the Parquet file to be processed.')
parser.add_argument('--output_dir', type=str, help='The folder to store the resulting CSV files.')

# Parse the command-line arguments
args = parser.parse_args()

# Access the argument values
to_process_path = args.to_process
output_dir = args.output_dir

print(output_dir)



parquet_file_path = to_process_path

# Use PyArrow to read the Parquet file
table = pq.read_table(parquet_file_path)

# Convert the PyArrow Table to a Pandas DataFrame
dataframe = table.to_pandas()

print(dataframe)

dataframe['unit'].value_counts()

df=dataframe.copy()


# Assuming 'df' is your original dataframe
# Convert the 'timestamp' column to datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Calculate the time difference between consecutive data points
df['time_diff'] = df['timestamp'].diff()

# Identify the start of a new trip based on the time difference threshold (7 hours)
new_trip_mask = df['time_diff'] > timedelta(hours=7)

# Assign a unique trip ID to each trip
df['trip_id'] = new_trip_mask.cumsum()

# Split the dataframe into individual dataframes based on trip ID
trip_dataframes = [group for _, group in df.groupby('trip_id')]

# Optional: Remove the temporary columns 'time_diff' and 'trip_id' if you don't need them
df = df.drop(['time_diff', 'trip_id'], axis=1)

output_dir=output_dir+"/"

len(trip_dataframes)


previous_unit_value = None  # Initialize with None for the first iteration

for trip_df in trip_dataframes:
    # Check if the 'unit' value is different from the previous iteration
    if trip_df['unit'].iloc[0] != previous_unit_value:
        i = 0  # Reset i to 0 for a new 'unit' value
    else:
        # Increment i for the same 'unit' value
        i += 1

    # Generate the CSV file name using the specified pattern
    csv_file_name = f"{trip_df['unit'].iloc[0]}_{i}.csv"
    
    trip_df['timestamp_rfc3339'] = trip_df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    trip_df.drop(columns={'timestamp'}, inplace=True)
    trip_df.rename(columns={'timestamp_rfc3339': 'timestamp'}, inplace=True)

    # Save the dataframe to CSV
    trip_df[['longitude', 'latitude', 'timestamp']].to_csv(output_dir + csv_file_name, index=False)
    print(f"Trip {i} saved to {csv_file_name}")

    # Update previous_unit_value for the next iteration
    previous_unit_value = trip_df['unit'].iloc[0]
