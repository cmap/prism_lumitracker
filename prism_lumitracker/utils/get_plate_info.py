import os
import pymysql
import pandas as pd
import s3fs

'''
Uses the lims database to look up currently scanning plates
and returns a dictionary of scanner dictionaries containing
plates names and csv file paths (csvs generated from 
lumitracker classic)

John Davis
'''

fs = s3fs.S3FileSystem(anon=False)


def get_plate_names_with_scanning_value_one(connection):
    with connection.cursor() as cursor:
        query = "SELECT newest_plate, scanner_id FROM scanner WHERE is_active = 1"
        cursor.execute(query)
        result = cursor.fetchall()
        scanners = [{'plate': row[0], 'csv_path': None} for row in result]
    return scanners, [row[1] for row in result]


def get_s3_csv_file(plate_name):
    bucket = "lumitracker.clue.io"
    prefix = plate_name

    files = fs.ls(f"{bucket}/{prefix}")
    for file in files:
        if plate_name in file:
            return file
    return None


def parse_csv_file(file_path):
    with fs.open(file_path, "r") as file:
        for line in file:
            if line.strip():
                # Process non-blank line
                pass


def main():
    db_host = 'lims.c2kct5xnoka4.us-east-1.rds.amazonaws.com'
    db_user = 'jdavis'
    db_password = 'DU;6tsb$;BFc>)'
    db_name = 'lims'

    connection = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        db=db_name,
    )

    scanners, scanner_ids = get_plate_names_with_scanning_value_one(connection)
    scanner_dict = {}

    for scanner, scanner_id in zip(scanners, scanner_ids):
        plate_name = scanner['plate']
        csv_file = get_s3_csv_file(plate_name)
        if csv_file:
            scanner['csv_path'] = csv_file
            parse_csv_file(csv_file)
            scanner_dict[f"scanner_{scanner_id}"] = scanner

    connection.close()

    print(scanner_dict)


if __name__ == "__main__":
    main()
