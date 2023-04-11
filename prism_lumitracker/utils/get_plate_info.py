import pymysql
import s3fs
import requests
import os
import pandas as pd
from urllib.parse import quote

'''
Uses the lims database to look up currently scanning plates
and returns a dictionary of scanner dictionaries containing
plates names and csv file paths (csvs generated from 
lumitracker classic)

John Davis
'''

fs = s3fs.S3FileSystem(anon=False)
DB_HOST = 'lims.c2kct5xnoka4.us-east-1.rds.amazonaws.com'
DB_NAME = 'lims'
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
API_URL = 'https://api.clue.io/api/'
API_KEY = os.environ.get('API_KEY')
BUILDS_URL = API_URL + 'data_build_types/prism-builds'


def get_plate_map_df(pert_plate, replicate):
    base_url = "https://api.clue.io/api/v_plate_map_src"
    api_key = os.environ.get("API_KEY")

    filter_param = f'{{"where": {{"pert_plate": "{pert_plate}", "replicate": "{replicate}"}}}}'
    encoded_filter_param = quote(filter_param)
    request_url = f"{base_url}?filter={encoded_filter_param}"

    headers = {
        "Accept": "application/json",
        "user_key": api_key
    }

    response = requests.get(request_url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        return df
    else:
        raise Exception(f"Request failed with status code {response.status_code}")


def get_plate_names_with_scanning_value_one(connection):
    with connection.cursor() as cursor:
        query = """
        SELECT scanner.newest_plate, scanner.scanner_id, plate.det_plate, plate.pert_plate, plate.cell_id, plate.pert_time, plate.replicate, plate.bead_batch_id
        FROM scanner
        JOIN plate ON plate.det_plate = scanner.newest_plate
        WHERE scanner.is_active = 1
        """
        cursor.execute(query)
        result = cursor.fetchall()
        scanners = [{'plate': row[0], 'csv_path': None, 'det_plate': row[2], 'pert_plate': row[3], 'cell_id': row[4],
                     'pert_time': row[5], 'replicate': row[6], 'beadset': row[7]} for row in result]
        if scanners:
            print(f"Found {len(scanners)} active scanners")
            print(f"Scanning plates: {[scanner['plate'] for scanner in scanners]}")
        else:
            print("No active scanners found")
    return scanners, [row[1] for row in result]


def get_s3_csv_file(plate_name):
    bucket = "lumitracker.clue.io"
    prefix = plate_name

    files = fs.ls(f"{bucket}/{prefix}")
    for file in files:
        if plate_name in file:
            return file
    return None


def get_scanner_dict():
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
    )

    scanners, scanner_ids = get_plate_names_with_scanning_value_one(connection)
    scanner_dict = {}

    for scanner, scanner_id in zip(scanners, scanner_ids):
        plate_name = scanner['plate']
        csv_file = get_s3_csv_file(plate_name)
        if csv_file:
            scanner['csv_path'] = csv_file
            scanner_dict[f"scanner_{scanner_id}"] = scanner

    connection.close()
    return scanner_dict
