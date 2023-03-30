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
    if not plate_dict[scanner]['csv_path']:
        break
    # Read in jcsv file
    path = plate_dict[scanner]['csv_path']
    df = parse_csv.parse_csv(path)
    df = df[df['count'] != 0]
    pert_plate = plate_dict[scanner]['pert_plate']
    replicate = plate_dict[scanner]['replicate']
    df['pert_plate'] = pert_plate
    df['replicate'] = replicate

    # Get plate map if applicable
    print(f"Looking for plate map for {pert_plate}_{replicate}...")
    plate_map = get_plate_info.get_plate_map_df(pert_plate, replicate)
    if plate_map.shape[0] > 0:
        df = df.merge(plate_map,
                      on=['pert_plate', 'replicate','pert_well'],
                      how='left')

    # Get scanning plate name
    plate = plate_dict[scanner]['plate']
    det_plate = plate_dict[scanner]['det_plate']

    # Get canonical name if it exists
    if plate_dict[scanner].get('pert_plate') is not None:
        canonical_plate = f"{plate_dict[scanner]['pert_plate']}_{plate_dict[scanner]['cell_id']}_{plate_dict[scanner]['pert_time']}_{plate_dict[scanner]['replicate']}_{plate_dict[scanner]['beadset']}"
    else:
        canonical_plate = None

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

    if 'pert_type' in df.columns:
        generate_figures.generate_box_plots(df,
                                            ctl_analytes,
                                            det_name=det_plate,
                                            canonical_name=canonical_plate,
                                            pert_type=['ctl_vehicle','ctl_untrt'])
