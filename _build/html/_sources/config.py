"""
config.py: Import necessary libraries and perform initial setup configurations.
"""

# Libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import statsmodels.api as sm
import statsmodels.formula.api as smf
import wrds
from pandas_datareader import data as pdr
from datetime import datetime
from IPython.display import display
from scipy.stats import zscore
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
from statsmodels.regression.linear_model import OLS
from statsmodels.regression.rolling import RollingOLS
from statsmodels.tools.tools import add_constant
from statsmodels.stats.sandwich_covariance import cov_hac
from statsmodels.iolib.summary2 import summary_col
from sklearn.impute import SimpleImputer
from pandas_datareader import data as pdr
from pandas.tseries.offsets import MonthEnd
from tabulate import tabulate

# Configurations
## We want to keep outputs clean and readable, so we will supress warnings
import warnings
warnings.filterwarnings('ignore')
