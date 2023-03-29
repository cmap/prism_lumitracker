#! /usr/bin/env python
"""
parse_csv.py

Takes a luminex csv file containing MFI and COUNT values and parses
into a long form table for use with lumiqc

John Davis
"""

import numpy as np
import pandas as pd
import csv
import boto3
from io import StringIO


def format_well(location):
    well = location.split('(')[1].split(')')[0].split(',')[1]
    letter = well[0]
    number = well[1:3].zfill(2)
    return letter + number


def make_long_table(df, metric):
    analyte_ids = [int(col.split()[-1]) for col in df.columns if col.startswith('Analyte')]
    if metric == "Median":
        result = pd.melt(df, id_vars=['Location'], value_vars=[f'Analyte {i}' for i in analyte_ids],
                         var_name='analyte_id',
                         value_name='mfi')
        result['Location'] = result['Location'].apply(format_well)  # reformat Location column
        result['logMFI'] = np.log2(result['mfi'] + 1)
        return result.rename(columns={'Location': 'pert_well'})
    elif metric == "Count":
        result = pd.melt(df, id_vars=['Location'], value_vars=[f'Analyte {i}' for i in analyte_ids],
                         var_name='analyte_id',
                         value_name='count')
        result['Location'] = result['Location'].apply(format_well)  # reformat Location column
        return result.rename(columns={'Location': 'pert_well'})


def make_df(s3_uri, start, end, metric):
    s3 = boto3.client('s3')
    bucket_name, key_name = s3_uri.replace('s3://', '').split('/', 1)

    # Get the object containing the CSV file
    obj = s3.get_object(Bucket=bucket_name, Key=key_name)

    # Read the CSV data into a string buffer
    csv_data = obj['Body'].read().decode('utf-8')
    csv_buffer = StringIO(csv_data)

    # Read the rows in the specified range into a list of dictionaries
    rows = []
    csv_buffer.seek(0)
    reader = csv.reader(csv_buffer)
    for i, row in enumerate(reader):
        if i == start[metric] - 1:
            col_names = row
        elif start[metric] <= i < end[metric]:
            if len(row) == len(col_names):
                rows.append(dict(zip(col_names, row)))

    # Create a Pandas dataframe from the list of dictionaries
    df = pd.DataFrame.from_records(rows)

    # Drop columns that are not needed
    df.drop(columns=['Sample', 'Total Events'], inplace=True)

    # Convert numeric columns to float data type
    numeric_cols = [col for col in df.columns if col != 'Location']
    df[numeric_cols] = df[numeric_cols].replace('', np.nan).astype(float).fillna(0)

    return df


def get_start_end_rows(s3_uri):
    s3 = boto3.client('s3')
    bucket_name, key_name = s3_uri.replace('s3://', '').split('/', 1)

    # Get the object containing the CSV file
    obj = s3.get_object(Bucket=bucket_name, Key=key_name)

    # Read the CSV data into a string buffer
    csv_data = obj['Body'].read().decode('utf-8')
    csv_buffer = StringIO(csv_data)

    # Count the number of rows in the CSV file
    count_end_row = count_csv_rows(s3_uri)
    print(f'The CSV file has {count_end_row} rows.')

    # Find the start and end rows for Median and Count
    start_rows = {'Median': None, 'Count': None}
    end_rows = {'Median': None, 'Count': None}
    csv_buffer.seek(0)
    reader = csv.reader(csv_buffer)
    for i, row in enumerate(reader):
        if 'DataType:' and 'Median' in row:
            start_rows['Median'] = i + 2
        elif 'DataType:' and 'Count' in row:
            start_rows['Count'] = i + 2
            end_rows['Median'] = i - 1
            end_rows['Count'] = count_end_row
    print(f'Median values begin on row {start_rows["Median"]}')
    print(f'Median values end on row {end_rows["Median"]}')
    print(f'Count values begin on row {start_rows["Count"]}')
    print(f'Count values end on row {end_rows["Count"]}')

    return start_rows, end_rows


def count_csv_rows(s3_uri):
    # Parse the S3 URI
    s3_bucket, s3_key = s3_uri.replace("s3://", "").split("/", 1)

    # Create an S3 client
    s3 = boto3.client("s3")

    # Read the CSV file from the S3 bucket
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    csvfile = response['Body'].read().decode('utf-8')

    # Use StringIO to convert the CSV content to a file-like object
    csvfile = StringIO(csvfile)

    # Read and count the rows
    reader = csv.reader(csvfile)
    count = sum(1 for _ in reader)
    return count


def parse_csv(csv_path):
    csv_path = csv_path
    print('Reading ' + csv_path + '...')
    start_line, end_line = get_start_end_rows(csv_path)
    med_df = make_df(csv_path, start=start_line, end=end_line, metric="Median")
    cnt_df = make_df(csv_path, start=start_line, end=end_line, metric="Count")
    med_df = make_long_table(med_df, "Median")
    cnt_df = make_long_table(cnt_df, "Count")
    res = med_df.merge(cnt_df, on=['pert_well', 'analyte_id'], how='outer')
    res.fillna(0, inplace=True)
    return res
