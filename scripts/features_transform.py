import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def features_transform(input_path, output_path):
    logging.info('Features Transform')
    
    logging.info('Reading data from parquet file')
    df = pd.read_parquet(input_path)
    
    # Sort the DataFrame by 'Symbol' and 'Date' in ascending order
    df.sort_values(['Symbol', 'Date'], ascending=True, inplace=True)
    
    logging.info('Calculate vol_moving_avg')
    # Calculate the moving average of 'Volume' for each 'Symbol' using a rolling window of 30 days
    df['vol_moving_avg'] = df.groupby('Symbol')['Volume'].rolling(window=30, min_periods=1).mean().reset_index(0, drop=True)
    
    logging.info('Calculate adj_close_rolling_med')
    # Calculate the rolling median of 'Adj Close' for each 'Symbol' using a specified window (e.g., 30 days)
    df['adj_close_rolling_med'] = df.groupby('Symbol')['Adj Close'].rolling(window=30, min_periods=1).median().reset_index(0, drop=True)
    
    output_filepath = 'etfs_stocks.parquet'
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        
    logging.info('Writing data to stage parquet file')
    
    # reduce the size of the output file
    cols = ['Date','Volume','vol_moving_avg', 'adj_close_rolling_med']
    df = df[cols]
    
    df.to_parquet(os.path.join(output_path,output_filepath), index=False)
    
    return True