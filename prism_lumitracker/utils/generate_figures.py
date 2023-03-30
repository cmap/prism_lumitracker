import seaborn as sns
import matplotlib.pyplot as plt
import os
from io import BytesIO
import boto3

bucket = 'lumitracker.clue.io'


def upload_plot_to_s3(bucket_name, prefix, plot, plot_filename):
    s3 = boto3.client('s3')

    # Ensure the prefix ends with a slash
    if not prefix.endswith('/'):
        prefix += '/'

    # Combine the prefix and plot filename to create the S3 key
    s3_key = f"{prefix}{plot_filename}"
    print(s3_key)

    # Save the plot to a BytesIO object as a PNG
    plot_buffer = BytesIO()
    plot.savefig(plot_buffer, format='png')
    plot_buffer.seek(0)

    # Upload the plot to S3
    s3.upload_fileobj(plot_buffer, bucket_name, s3_key)
    print(f"Uploaded plot '{plot_filename}' to '{s3_key}' in bucket '{bucket_name}'.")


def generate_cal_curve(df, ctl_analytes, det_name, canonical_name=None):
    # Group the data by a categorical variable
    data = df[(df.analyte_id.isin(ctl_analytes)) & (df['count'] != -666)]

    # Set hue column to use if it exists
    hue_col = 'pert_type'

    # Create a new plot and plot each group separately
    plt.figure()
    if hue_col in data.columns:
        filtered = data[~data.pert_type.isna()]
        order = sorted(list(filtered['pert_type'].unique()))
        sns.lineplot(data=filtered,
                     x='analyte_id',
                     y='logMFI',
                     hue=hue_col,
                     legend=True,
                     ci=False,
                     hue_order=order)
    else:
        sns.lineplot(data=data,
                     x='analyte_id',
                     y='logMFI',
                     legend=False,
                     ci=False)
    plt.xticks(rotation=45)
    min_ylim = 6
    plt.ylim(min_ylim, plt.ylim()[1])

    if canonical_name:
        plt.title(canonical_name + ' control analytes')
    else:
        plt.title(det_name + ' control analytes')

    # Get the current figure
    current_figure = plt.gcf()

    # Set the filename
    filename = 'calibplot.png'

    upload_plot_to_s3(bucket_name=bucket,
                      prefix=det_name,
                      plot=current_figure,
                      plot_filename=filename)

    plt.close(current_figure)


def make_matrix_df(df, metric):
    grouped = df.groupby(['pert_well']).median().reset_index()
    grouped['row'] = grouped['pert_well'].str[0]
    grouped['col'] = grouped['pert_well'].str[1:3]

    matrix = grouped.pivot(columns=['col'],
                           values=metric,
                           index='row')

    return matrix


def generate_lmfi_heatmap(df, ctl_analytes, det_name, canonical_name=None):
    """
    Generates a heatmap of the logMFI values for a plate.

    Args:
    - df (pd.DataFrame): A longform dataframe containing the logMFI values for the plate.
    - ctl_analytes (list): A list of analyte IDs that correspond to control samples, which will be excluded from the heatmap.
    - plate_name (str): The name of the plate, which will be used as the title of the heatmap.
    - output_dir (str): The directory where the output heatmap should be saved.

    Returns: None
    """
    # Subset the data to only cell lines and transform to a matrix
    lmfi = make_matrix_df(df[~df.analyte_id.isin(ctl_analytes)],
                          metric="logMFI")

    # Generate the heatmap
    plt.figure()
    sns.heatmap(lmfi, vmin=0, vmax=16)

    # Set the title of the heatmap
    if canonical_name:
        plt.title(canonical_name + ' logMFI')
    else:
        plt.title(det_name + ' logMFI')

    # Get the current figure
    current_figure = plt.gcf()

    # Set the filename
    filename = 'qc_level10.png'

    upload_plot_to_s3(bucket_name=bucket,
                      prefix=det_name,
                      plot=current_figure,
                      plot_filename=filename)

    plt.close(current_figure)

    # Save the heatmap to a file
    # plt.savefig(os.path.join(output_dir, det_name, 'lmfi.png'))

    # Close the plot to free up memory
    plt.close()


def generate_count_heatmap(df, ctl_analytes, det_name, canonical_name=None):
    """
    Generates a heatmap of the count values for a plate.

    Args:
    - df (pd.DataFrame): A longform dataframe containing the count values for the plate.
    - ctl_analytes (list): A list of analyte IDs that correspond to control samples, which will be excluded from the heatmap.
    - det_name (str): The machine readable name of the plate, which will be used as the title of the heatmap.
    - output_dir (str): The directory where the output heatmap should be saved.

    Returns: None
    """
    # Subset the data to only cell lines and transform to a matrix
    count = make_matrix_df(df[~df.analyte_id.isin(ctl_analytes)],
                           metric="count")

    # Generate the heatmap
    plt.figure()
    sns.heatmap(count, vmin=0, vmax=35)

    # Set the title of the heatmap
    if canonical_name:
        plt.title(canonical_name + ' count')
    else:
        plt.title(det_name + 'count')

    # Get the current figure
    current_figure = plt.gcf()

    # Set the filename
    filename = 'plate_count.png'

    upload_plot_to_s3(bucket_name=bucket,
                      prefix=det_name,
                      plot=current_figure,
                      plot_filename=filename)

    plt.close(current_figure)

    # Save the heatmap to a file
    # plt.savefig(os.path.join(output_dir, plate_name, 'count.png'))

    # Close the plot to free up memory
    plt.close()


def generate_box_plots(df, ctl_analytes, det_name, canonical_name=None, pert_type='clt_vehicle'):
    # Filter the data to remove control beads and yet to be scanned wells, subset to pert_type
    data = df[(~df.analyte_id.isin(ctl_analytes)) & (df['count'] != -666) & (df.pert_type.isin(pert_type))]

    # Create a new plot and plot each group separately
    plt.figure()
    sns.boxplot(data=data,
                 x='pert_well',
                 y='logMFI',
                color='dodgerblue',
                fliersize=0)
    plt.xticks(rotation=45)
    plt.ylim(6, 16)
    if canonical_name:
        plt.title(f"{canonical_name} {pert_type} logMFI")
    else:
        plt.title(f"{det_name} {pert_type} logMFI")

    # Get the current figure
    current_figure = plt.gcf()

    # Set the filename
    filename = 'quantiles_raw.png'

    upload_plot_to_s3(bucket_name=bucket,
                      prefix=det_name,
                      plot=current_figure,
                      plot_filename=filename)

    plt.close(current_figure)
