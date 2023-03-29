import seaborn as sns
import matplotlib.pyplot as plt
import os


def generate_cal_curve(df, ctl_analytes, plate_name, output_dir):
    # Group the data by a categorical variable
    grouped = df[df.analyte_id.isin(ctl_analytes)].groupby('pert_well')

    # Create a new plot and plot each group separately
    plt.figure()
    for name, group in grouped:
        sns.lineplot(data=group,
                     x='analyte_id',
                     y='logMFI',
                     label=name,
                     legend=False)
    plt.xticks(rotation=45)
    plt.ylim(4, 16)
    plt.savefig(os.path.join(output_dir, plate_name, 'cal.png'))


def make_matrix_df(df, metric):
    grouped = df.groupby(['pert_well']).median().reset_index()
    grouped['row'] = grouped['pert_well'].str[0]
    grouped['col'] = grouped['pert_well'].str[1:3]

    matrix = grouped.pivot(columns=['col'],
                           values=metric,
                           index='row')

    return matrix


def generate_lmfi_heatmap(df, ctl_analytes, plate_name, output_dir):
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
    plt.title(plate_name + ' logMFI')

    # Save the heatmap to a file
    plt.savefig(os.path.join(output_dir, plate_name, 'lmfi.png'))

    # Close the plot to free up memory
    plt.close()



def generate_count_heatmap(df, ctl_analytes, plate_name, output_dir):
    """
    Generates a heatmap of the count values for a plate.

    Args:
    - df (pd.DataFrame): A longform dataframe containing the count values for the plate.
    - ctl_analytes (list): A list of analyte IDs that correspond to control samples, which will be excluded from the heatmap.
    - plate_name (str): The name of the plate, which will be used as the title of the heatmap.
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
    plt.title(plate_name + ' Count')

    # Save the heatmap to a file
    #os.mkdir(os.path.join(output_dir, plate_name))
    plt.savefig(os.path.join(output_dir, plate_name, 'count.png'))

    # Close the plot to free up memory
    plt.close()

