import utils.parse_csv as parse_csv
import utils.get_plate_info as get_plate_info
import seaborn as sns
import plotly.express as px
import numpy as np
import matplotlib.pyplot as plt
import s3fs

fs = s3fs.S3FileSystem(anon=False)
plate_dict = get_plate_info.get_scanner_dict()


def generate_figures():
    for scanner in plate_dict:
        plate = plate_dict[scanner]['plate']
        path = plate_dict[scanner]['csv_path']
        df = parse_csv.parse_csv(csv_path='s3://' + path,
                                 out_path=path)
        print(plate)
        print(df)


generate_figures()

