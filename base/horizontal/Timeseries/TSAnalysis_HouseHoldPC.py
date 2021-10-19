# Databricks notebook source
# MAGIC %md # TimeSeries Analysis using Household Consumption Data

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 15, 6

# COMMAND ----------

# MAGIC %sh mv /household_power_consumptions.csv /databricks/driver/
# MAGIC   

# COMMAND ----------

# MAGIC %fs cp /mnt/vedant-demo/timeseries/household_power_consumptions.csv file:/databricks/driver/

# COMMAND ----------

data = pd.read_csv('/databricks/driver/household_power_consumptions.csv')
print (data.head())
print ('\n Data Types:')
print (data.dtypes)

# COMMAND ----------

dateparse = lambda dates: pd.datetime.strptime(dates, '%d/%m/%Y %H:%M:%S')
data = pd.read_csv('/databricks/driver/household_power_consumptions.csv', parse_dates=['Time'], index_col='Time',date_parser=dateparse)
print (data.head())

# COMMAND ----------

#check datatype of index
data.index

# COMMAND ----------

#convert to time series:
ts = data['Global_intensity']
ts.head(10)

# COMMAND ----------

# MAGIC %md ### Indexing TS arrays:

# COMMAND ----------

ts['2006-12-17']

# COMMAND ----------

# MAGIC %md # Checking for stationarity

# COMMAND ----------

# MAGIC %md ### Plot the time-series

# COMMAND ----------

def toFloat(x):
  try:
    x1 = float(x)
    return x1
  except:
    return 0
ts = ts.map(lambda x: toFloat(x))
fig, ax = plt.subplots()
ax.plot(ts)
display(fig)

# COMMAND ----------

# MAGIC %md ### Function for testing stationarity

# COMMAND ----------

from statsmodels.tsa.stattools import adfuller
def test_stationarity(timeseries):
    
    #Determing rolling statistics
    rolmean = pd.rolling_mean(timeseries, window=12)
    rolstd = pd.rolling_std(timeseries, window=12)
    fig, ax = plt.subplots()

    #Plot rolling statistics:
    orig = ax.plot(timeseries, color='blue',label='Original')
    mean = ax.plot(rolmean, color='red', label='Rolling Mean')
    std = ax.plot(rolstd, color='black', label = 'Rolling Std')
    ax.legend(loc='best')
    #ax.title('Rolling Mean & Standard Deviation')
    #plt.show(block=False)
    display(fig)
    
    #Perform Dickey-Fuller test:
    print ('Results of Dickey-Fuller Test:')
    dftest = adfuller(timeseries, autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
    for key,value in dftest[4].items():
        dfoutput['Critical Value (%s)'%key] = value
    print (dfoutput)

# def toFloat(x):
#   try:
#     x1 = float(x)
#     return x1
#   except:
#     return 0
# ts = ts.map(lambda x: toFloat(x))
test_stationarity(ts)

# COMMAND ----------

# MAGIC %md # Making TS Stationary

# COMMAND ----------

# MAGIC %md ## Estimating & Eliminating Trend

# COMMAND ----------

ts_log = np.log(ts)
fig, ax = plt.subplots()
ax.plot(ts_log)
display(fig)


# COMMAND ----------

# MAGIC %md ## Smoothing:

# COMMAND ----------

# MAGIC %md ### Moving average

# COMMAND ----------

moving_avg = pd.rolling_mean(ts_log,1440)
ax.plot(ts_log)
ax.plot(moving_avg, color='red')
display(fig)

# COMMAND ----------

ts_log_moving_avg_diff = ts_log - moving_avg
ts_log_moving_avg_diff.head(10)

# COMMAND ----------

ts_log_moving_avg_diff.dropna(inplace=True)
ts_log_moving_avg_diff.head()

# COMMAND ----------

test_stationarity(ts_log_moving_avg_diff)

# COMMAND ----------

# MAGIC %md ## Eliminating Trend and Seasonality

# COMMAND ----------

# MAGIC %md ### Differencing:

# COMMAND ----------

#Take first difference:
ts_log_diff = ts_log - ts_log.shift()
ax.plot(ts_log_diff)
display(fig)

# COMMAND ----------

ts_log_diff.dropna(inplace=True)
#test_stationarity(ts_log_diff)

# COMMAND ----------

# MAGIC %md # Final Forecasting

# COMMAND ----------

# MAGIC %md ### ACF & PACF Plots

# COMMAND ----------

from statsmodels.tsa.arima_model import ARIMA

# COMMAND ----------

#ACF and PACF plots:
from statsmodels.tsa.stattools import acf, pacf  

lag_acf = acf(ts_log_diff, nlags=20)
lag_pacf = pacf(ts_log_diff, nlags=20, method='ols')

#Plot ACF:    
ax.subplot(121)    
ax.plot(lag_acf)
ax.axhline(y=0,linestyle='--',color='gray')
ax.axhline(y=-1.96/np.sqrt(len(ts_log_diff)),linestyle='--',color='gray')
ax.axhline(y=1.96/np.sqrt(len(ts_log_diff)),linestyle='--',color='gray')
#plt.title('Autocorrelation Function')

#Plot PACF:
ax.subplot(122)
ax.plot(lag_pacf)
ax.axhline(y=0,linestyle='--',color='gray')
ax.axhline(y=-1.96/np.sqrt(len(ts_log_diff)),linestyle='--',color='gray')
ax.axhline(y=1.96/np.sqrt(len(ts_log_diff)),linestyle='--',color='gray')
#plt.title('Partial Autocorrelation Function')
#plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md ### AR Model:

# COMMAND ----------

model = ARIMA(ts_log, order=(2, 1, 0))  
results_AR = model.fit(disp=-1)  
plt.plot(ts_log_diff)
plt.plot(results_AR.fittedvalues, color='red')
plt.title('RSS: %.4f'% sum((results_AR.fittedvalues-ts_log_diff)**2))

# COMMAND ----------

# MAGIC %md ### MA Model

# COMMAND ----------

model = ARIMA(ts_log, order=(0, 1, 1))  
results_MA = model.fit(disp=-1)  
plt.plot(ts_log_diff)
plt.plot(results_MA.fittedvalues, color='red')
plt.title('RSS: %.4f'% sum((results_MA.fittedvalues-ts_log_diff)**2))

# COMMAND ----------

# MAGIC %md ### ARIMA Model:

# COMMAND ----------

model = ARIMA(ts_log, order=(2, 1, 1))  
results_ARIMA = model.fit(disp=-1)  
plt.plot(ts_log_diff)
plt.plot(results_ARIMA.fittedvalues, color='red')
plt.title('RSS: %.4f'% sum((results_ARIMA.fittedvalues-ts_log_diff)**2))

# COMMAND ----------

# MAGIC %md ### Convert to original scale:

# COMMAND ----------

predictions_ARIMA_diff = pd.Series(results_ARIMA.fittedvalues, copy=True)
print (predictions_ARIMA_diff.head())

# COMMAND ----------

predictions_ARIMA_diff_cumsum = predictions_ARIMA_diff.cumsum()
print (predictions_ARIMA_diff_cumsum.head())

# COMMAND ----------

predictions_ARIMA_log = pd.Series(ts_log.ix[0], index=ts_log.index)
predictions_ARIMA_log = predictions_ARIMA_log.add(predictions_ARIMA_diff_cumsum,fill_value=0)
predictions_ARIMA_log.head()

# COMMAND ----------

predictions_ARIMA = np.exp(predictions_ARIMA_log)
plt.plot(ts)
plt.plot(predictions_ARIMA)
plt.title('RMSE: %.4f'% np.sqrt(sum((predictions_ARIMA-ts)**2)/len(ts)))

# COMMAND ----------

