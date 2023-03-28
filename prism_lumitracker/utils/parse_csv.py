#! /usr/bin/env python
"""
parse_csv.py

Takes a luminex csv file containing MFI and COUNT values and parses
into a long form table for use with lumiqc

John Davis
"""

import os

import numpy as np
import pandas as pd
import argparse
import sys
import csv


def build_parser():
    parser = argparse.ArgumentParser()
    required = parser.add_argument_group('required arguments')
    required.add_argument('--csv', '-c', help='Path to CSV', required=True)
    required.add_argument('--out', '-o', help='Output path. Defualt is current working directory.', default=os.getcwd())
    return parser


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


def make_df(csv_path, start, end, metric):
    rows = []
    with open(csv_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for i, row in enumerate(reader):
            if i == start[metric] - 1:
                col_names = row
            elif start[metric] <= i < end[metric]:
                if len(row) == len(col_names):
                    rows.append(dict(zip(col_names, row)))
    df = pd.DataFrame.from_records(rows)
    df.drop(columns=['Sample', 'Total Events'], inplace=True)

    # Convert numeric columns to float data type
    numeric_cols = [col for col in df.columns if col != 'Location']
    df[numeric_cols] = df[numeric_cols].replace('', np.nan).astype(float).fillna(0)
    return df


def get_start_end_rows(csv_path):
    start_rows = {'Median': None, 'Count': None}
    end_rows = {'Median': None, 'Count': None}
    count_end_row = count_csv_rows(csv_path)
    print(f'The CSV file has {count_end_row} rows.')

    with open(csv_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
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


def count_csv_rows(csv_path):
    with open(csv_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        count = sum(1 for _ in reader)
    return count


def main(args):
    csv_path = args.csv
    out_path = args.out
    print('Reading ' + csv_path + '...')
    start_line, end_line = get_start_end_rows(csv_path)
    med_df = make_df(csv_path, start=start_line, end=end_line, metric="Median")
    cnt_df = make_df(csv_path, start=start_line, end=end_line, metric="Count")
    med_df = make_long_table(med_df, "Median")
    cnt_df = make_long_table(cnt_df, "Count")
    res = med_df.merge(cnt_df, on=['pert_well', 'analyte_id'], how='outer')
    return res


if __name__ == "__main__":
    args = build_parser().parse_args(sys.argv[1:])

    main(args)
