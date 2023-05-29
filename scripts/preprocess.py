import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preprocess(input_path, output_path):

    etfs_folder = os.path.join(input_path, 'etfs')
    stocks_folder = os.path.join(input_path, 'stocks')
    dfs = []  # List to store individual DataFrames

    # Process ETFs
    for file in os.listdir(etfs_folder):
        if file.endswith('.csv'):
            filepath = os.path.join(etfs_folder, file)
            symbol = filepath.split('/')[-1].split('.')[0]
            df = pd.read_csv(filepath, parse_dates=['Date'])
            df['Symbol'] = symbol
            dfs.append(df)

    # Process stocks
    for file in os.listdir(stocks_folder):
        if file.endswith('.csv'):
            filepath = os.path.join(stocks_folder, file)
            symbol = filepath.split('/')[-1].split('.')[0]
            df = pd.read_csv(filepath, parse_dates=['Date'])
            df['Symbol'] = symbol
            dfs.append(df)
    
    # Merge all DataFrames into a single DataFrame
    merged_df = pd.concat(dfs, ignore_index=True)
    
    # Get the metadata so we can join the Security Names with the symbols
    mapping_filepath = os.path.join(input_path, 'symbols_valid_meta.csv')
    mapping_df = pd.read_csv(mapping_filepath)

    merged_df = pd.merge(merged_df, mapping_df[['Symbol', 'Security Name']], on='Symbol', how='left')

    # Check for null or empty values in all columns
    null_counts = merged_df.isnull().sum()
    empty_counts = (merged_df == '').sum()

    # Drop rows with null or empty values
    if (null_counts > 0).any() or (empty_counts > 0).any():
        merged_df = merged_df.dropna()
        
    # Perform data transformations on the merged DataFrame to ensure datatypes are correct.
    # Convert the date column to YYYY-MM-DD format
    merged_df.loc[:, 'Date'] = merged_df['Date'].dt.strftime('%Y-%m-%d')
    # Ensure the desired data types, floats for price, integers for volume, and strings for symbol.
    merged_df.loc[:, 'Symbol'] = merged_df['Symbol'].astype(str)
    merged_df.loc[:, 'Security Name'] = merged_df['Security Name'].astype(str)
    merged_df.loc[:, 'Open'] = merged_df['Open'].astype(float)
    merged_df.loc[:, 'High'] = merged_df['High'].astype(float)
    merged_df.loc[:, 'Low'] = merged_df['Low'].astype(float)
    merged_df.loc[:, 'Close'] = merged_df['Close'].astype(float)
    merged_df.loc[:, 'Adj Close'] = merged_df['Adj Close'].astype(float)
    merged_df.loc[:, 'Volume'] = merged_df['Volume'].astype(int)
    
    output_filepath = 'etfs_stocks.parquet'

    # Write the processed data to Parquet format
    merged_df.to_parquet(os.path.join(output_path,output_filepath), index=False)
    
    return True
