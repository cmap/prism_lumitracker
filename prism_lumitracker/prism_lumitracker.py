import utils.parse_csv as parse_csv
import utils.get_plate_info as get_plate_info
import utils.generate_figures as generate_figures

plate_dict = get_plate_info.get_scanner_dict()
ctl_analytes = ['Analyte ' + str(i) for i in range(1, 11)]

for scanner in plate_dict:
    df = parse_csv.parse_csv(plate_dict[scanner]['csv_path'])

    cal_curve = generate_figures.generate_cal_curve(df,ctl_analytes)
