#! /usr/bin/env python
'''
parse_csv.py

Takes a luminex csv file containing MFI and COUNT values and parses
into a long form table for use with lumiqc

John Davis
'''

import os
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


def main(args):
    csv_path = args.csv
    out_path = args.out
    print('Reading ' + csv_path + '...')

    start_rows = {'Median': None, 'Count': None}
    end_rows = {'Median': None, 'Count': None}

    count_end_row = count_csv_rows(csv_path)
    print(f'The CSV file has {count_end_row} rows.')

    with open(csv_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for i, row in enumerate(reader):
            if 'Median' in row[1]:
                start_rows['Median'] = i + 1
                print(f'Median values begin on row {start_rows["Median"]}')
            elif 'Count' in row[1]:
                start_rows['Count'] = i + 1
                end_rows['Median'] = i - 2
                end_rows['Count'] = count_end_row
                print(f'Median values end on row {end_rows["Median"]}')
                print(f'Count values begin on row {start_rows["Count"]}')
                print(f'Count values end on row {end_rows["Count"]}')


def count_csv_rows(csv_path):
    with open(csv_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        count = sum(1 for row in reader)
    return count


if __name__ == "__main__":
    args = build_parser().parse_args(sys.argv[1:])

    main(args)
