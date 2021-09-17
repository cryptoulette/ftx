import pandas as pd
from ftx import FtxClient
import time
import dateutil.parser as dp

client = FtxClient()

def get_historical_funding_2(future_name):

    new_results = True
    today = time.time()
    _one_day = 86400
    
    try: #if existing file
        existing_df = pd.read_pickle("./Funding_Data/" + str(future_name) + ".pkl")
        parsed_t = dp.parse(existing_df["time"].max())
        start_time = parsed_t.timestamp()
        timeseries = existing_df
    except FileNotFoundError: #if no existing file

        timeseries = pd.DataFrame()
        start_time = 1551358800
        
    while new_results == True:
        end_time = str(start_time + _one_day)
        api_result = client.get_hist_funding_rates(start_time, end_time, future_name)
        api_result = pd.DataFrame(api_result)
        timeseries = pd.concat([timeseries, api_result])
        timeseries.to_pickle("./Funding_Data/" + str(future_name) + ".pkl")
        timeseries = pd.read_pickle("./Funding_Data/" + str(future_name) + ".pkl")

        start_time += _one_day
        if start_time > today:
            new_results = False  
    
    
    return timeseries