import seaborn as sns
import plotly.express as px
import numpy as np
import matplotlib.pyplot as plt


def generate_cal_curve(df, ctl_analytes):
    plt.figure()
    sns.lineplot(data=df[df.analyte_id.isin(ctl_analytes)],
                       x='analyte_id',
                       y='logMFI')
    plt.savefig('/Users/jdavis/Desktop/cal_curve.png')
