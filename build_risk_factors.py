# Load monthly data downloaded from Goyal's website: https://sites.google.com/view/agoyal145
csv_riskfactors = 'Goyal_Monthly.csv'
riskfactors = pd.read_csv(csv_riskfactors)
# Now we transform the column yyyymm that looks like 187101.0 into two columns: year and month
## First drop the rows with missing values in the variable 'yyyymm'
riskfactors = riskfactors.dropna(subset=['yyyymm'])
riskfactors['yyyymm'] = riskfactors['yyyymm'].astype(int)
riskfactors['year'] = riskfactors['yyyymm'] // 100
riskfactors['month'] = riskfactors['yyyymm'] % 100
## Drop the yyyymm column for a cleaner dataframe
riskfactors = riskfactors.drop(columns=['yyyymm'])
## Set the columns 'year' and 'month' as the first and second columns
cols = riskfactors.columns.tolist()
cols = cols[-2:] + cols[:-2]
riskfactors = riskfactors[cols]
## Since we are interested in years 1941 onwards, we drop the rows with year less than 1941
riskfactors = riskfactors[riskfactors['year'] >= 1941]
# Replace commas and convert to float for 'Index'
riskfactors['Index'] = riskfactors['Index'].str.replace(',', '').astype(float)
# Now we need to index the data to move to a time series structure
riskfactors['date'] = riskfactors.apply(lambda x: datetime(int(x['year']), int(x['month']), 1), axis=1)
riskfactors = riskfactors.set_index('date') 
# Convert Rfree to the same scale as other returns
riskfactors['Rf'] = 1 + riskfactors['Rfree']
# Calculate log returns of stock price + dividends
riskfactors['Sp_log'] = np.log(riskfactors['Index'] + riskfactors['D12'])
riskfactors['Sp_log_lag'] = riskfactors['Sp_log'].shift(1)
riskfactors['Rm'] = riskfactors['Sp_log'] - riskfactors['Sp_log_lag']
# Compute excess market return, eRm
# Correction from previous explanation: eRm should be the market return minus the log of Rf
riskfactors['mktrf'] = riskfactors['Rm'] - np.log(riskfactors['Rf'])
# Compute dividend-price ratio, dp
riskfactors['dp'] = (riskfactors['D12'] / riskfactors['Index']) * 100
# Compute dividend yield, dy
riskfactors['dy'] = np.log(riskfactors['D12']) - np.log(riskfactors['Sp_log_lag'])
# Compute earnings-price ratio, ep
riskfactors['ep'] = np.log(riskfactors['E12']) - np.log(riskfactors['Index'])
# Drop NaN values that result from lagging and log operations
riskfactors = riskfactors.dropna(subset=['mktrf', 'dy', 'ep', 'dp'])
# Compute term = lty - tbl
riskfactors['term'] = riskfactors['AAA'] - riskfactors['tbl']
## Scaling term to be in percentage terms
riskfactors['term'] = riskfactors['term'] * 100
# Compute def: default yield spread: yield of BAA - AAA bond yield 
riskfactors['def'] = riskfactors['BAA'] - riskfactors['AAA']
## Scaling def to be in percentage terms
riskfactors['def'] = riskfactors['def'] * 100
# Rename 'tbl' to 'tb30' and drop 'tbl' 
riskfactors['tb30'] = riskfactors['tbl']
riskfactors = riskfactors.drop(columns=['tbl'])
# Let's first import the library we need to pull FRED data
# Annual growth in industrial production
industrial_production = pdr.get_data_fred('INDPRO', start=datetime(1941, 1, 1))
# The variable is a seasonally adjusted index with base in 2017=100, so we want to map it to an annual growth rate
industrial_production = industrial_production.resample('M').last()
industrial_production = industrial_production.pct_change(12)
industrial_production = industrial_production.dropna()
industrial_production = industrial_production.rename(columns={'INDPRO': 'ypl'})
# Real GDP growth
real_gdp = pdr.get_data_fred('GDPC1', start=datetime(1941, 1, 1))
real_gdp = real_gdp.resample('M').last()
real_gdp = real_gdp.pct_change(12)
real_gdp = real_gdp.dropna()
real_gdp = real_gdp.rename(columns={'GDPC1': 'realgnpg'})
### CRSP from pandas.tseries.offsets import MonthEnd
#! NOTE THAT THIS CELL WILL TAKE MORE THAN 2 MINS TO RUN BECAUSE IT IS PULLING A LARGE DATASET FROM WRDS (Millions of Obs.)
# Establish connection to WRDS
conn = wrds.Connection() # Input your WRDS username and password
# Define SQL query with necessary filters and join conditions applied upfront
sql_query = """
            SELECT a.permno, a.permco, a.date, b.shrcd, b.exchcd, 
                   CAST(a.ret AS FLOAT) AS ret, 
                   a.vol, a.shrout, 
                   ABS(a.prc) AS prc, a.hexcd, a.hsiccd, 
                   CAST(d.dlret AS FLOAT) AS dlret, d.dlstdt
            FROM crsp.msf AS a
            LEFT JOIN crsp.msenames AS b ON a.permno=b.permno AND b.namedt<=a.date AND a.date<=b.nameendt
            LEFT JOIN crsp.msedelist AS d ON a.permno=d.permno AND a.date=d.dlstdt
            WHERE a.date >= '1941-01-01' AND a.date <= '2023-12-31'
            AND b.shrcd IN (10, 11)
            """
crsp = conn.raw_sql(sql_query, date_cols=['date', 'dlstdt'])

# Convert dates to 'jdate' format directly
crsp['jdate'] = pd.to_datetime(crsp['date']).dt.to_period('M').dt.to_timestamp('M')
crsp['dljdate'] = pd.to_datetime(crsp['dlstdt']).dt.to_period('M').dt.to_timestamp('M') if 'dlstdt' in crsp else None
# Handling delisting returns and price adjustments in a more vectorized manner
crsp['ret_final'] = np.where(crsp['ret'].isna() & crsp['dlret'].notna(), crsp['dlret'], crsp['ret'])
crsp['ret_final'] = np.where(crsp['ret_final'].isna(), -1, crsp['ret_final'])
crsp['prc'] = np.where(crsp['ret_final'] == -1, 0, crsp['prc'])
# Calculate market equity
crsp['me'] = crsp['prc'] * crsp['shrout']
# Drop unnecessary columns in a more efficient manner
crsp.drop(['dlret', 'dlstdt', 'prc', 'shrout', 'ret', 'date'], axis=1, inplace=True)
# Sort and filter final dataset more efficiently
crsp.sort_values(by=['jdate', 'permco', 'me'], inplace=True)
crsp.reset_index(drop=True, inplace=True)
# For Value-Weighted Returns (vwretx)
# Sum of (return * market equity) divided by sum of market equity for each date
vwretx = crsp.groupby('jdate').apply(lambda df: (df['ret_final'] * df['me']).sum() / df['me'].sum())
# For Equally-Weighted Returns (ewretx)
# Simple mean of returns for each date
ewretx = crsp.groupby('jdate')['ret_final'].mean()
# Combine the value-weighted and equally-weighted returns into a single DataFrame
returns_df = pd.DataFrame({'jdate': vwretx.index, 'vwretx': vwretx.values, 'ewretx': ewretx.values})
# Now we want to merge the dataframes we have created so far. returns_df needs to use jdate as DateTimeIndex and be renamed "DATE" to match the FRED dataframes. 
# Rename jdate to DATE
returns_df = returns_df.rename(columns={'jdate': 'DATE'})
returns_df = returns_df.set_index('DATE')
returns_df.index = returns_df.index.to_period('M').to_timestamp('M')
# Merge with real_gdp and industrial_production into a single dataframe
df = returns_df.merge(real_gdp, how='left', left_index=True, right_index=True)
df = df.merge(industrial_production, how='left', left_index=True, right_index=True)
# Now we move from DATE (DateTimeIndex) being end of month to DATE being the beginning of the next month
df.index = df.index + MonthEnd(1)
# We rename the DateTimeIndex to date to match the riskfactors dataframe
df.index.name = 'date'
# Now we are ready to merge the dataframes and create an 'expanded' riskfactors dataframe
# First we push the beginning of the month to the end of the month for the riskfactors dataframe
riskfactors.index = riskfactors.index + MonthEnd(1)
# Step 1: Merge DataFrames on their indexes. We'll use an outer join to ensure no data is lost during the merge
merged_df = pd.merge(df, riskfactors, left_index=True, right_index=True, how='outer')
# Step 2: Select the columns we want to keep
riskfactors = merged_df[['dp', 'def', 'term', 'ypl', 'tb30', 'CRSP_SPvwx', 'realgnpg', 'mktrf', 'Rfree', 'AAA', 'BAA', 'vwretx', 'ewretx']]