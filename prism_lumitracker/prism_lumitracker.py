import os.path
import boto3
import utils.parse_csv as parse_csv
import utils.get_plate_info as get_plate_info
import utils.generate_figures as generate_figures

# Generate dictionary of plate names, jcsv paths and scanners
plate_dict = get_plate_info.get_scanner_dict()

# Define control analytes for later exclusion
ctl_analytes = ['Analyte ' + str(i) for i in range(1, 11)]

# Set the name of the S3 bucket and the directory path that you want to check
bucket_name = 'lumitracker.clue.io'

# Create a connection to the S3 service
s3 = boto3.client('s3')

for scanner in plate_dict:
    df = parse_csv.parse_csv(plate_dict[scanner]['csv_path'])

    plate = plate_dict[scanner]['plate']

    det_plate = plate_dict[scanner]['det_plate']

    if plate_dict[scanner].get('prefix') is not None:
        canonical_plate = f"{plate_dict[scanner]['prefix']}_{plate_dict[scanner]['replicate']}_{plate_dict[scanner]['beadset']}"
    else:
        canonical_plate = None

    if not os.path.exists("/Users/jdavis/Desktop/" + plate):
        os.mkdir("/Users/jdavis/Desktop/" + plate)

    # Check if the prefix exists
    # directory_exists = True
    # try:
    #    s3.head_object(Bucket=bucket_name, Key=directory_path)
    # except:
    #    directory_exists = False

    # Print the result
    # if directory_exists:
    #    print(f"Directory '{directory_path}' exists in bucket '{bucket_name}'")
    # else:
    #    print(f"Directory '{directory_path}' does not exist in bucket '{bucket_name}'")

    generate_figures.generate_cal_curve(df,
                                        ctl_analytes,
                                        det_name=det_plate,
                                        canonical_name=canonical_plate)

    generate_figures.generate_lmfi_heatmap(df,
                                           ctl_analytes,
                                           det_name=det_plate,
                                           canonical_name=canonical_plate)

    generate_figures.generate_count_heatmap(df,
                                            ctl_analytes,
                                            det_name=det_plate,
                                            canonical_name=canonical_plate)
