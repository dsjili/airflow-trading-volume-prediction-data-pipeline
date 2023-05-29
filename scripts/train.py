import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def train(input_path, output_path):
    
    data = pd.read_parquet(input_path)

    # Assume `data` is loaded as a Pandas DataFrame
    data['Date'] = pd.to_datetime(data['Date'])
    data.set_index('Date', inplace=True)

    # Remove rows with NaN values
    data.dropna(inplace=True)

    # Select features and target
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'

    X = data[features]
    y = data[target]

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Create a RandomForestRegressor model
    model = RandomForestRegressor(n_estimators=100, random_state=42)

    total_rows = len(data)
    window_size=1000000
    batch_size=100000
    
    # Train the model
    for start in range(0, total_rows - window_size, batch_size):
        end = start + window_size
        
        logging.info(f"Training batch {start} to {end}")
        
        # Perform feature selection and target assignment
        X = X_train[start:end]
        y = y_train[start:end]
        
        # Fit the model on the current batch of data
        model.fit(X, y)

    # Make predictions on test data
    y_pred = model.predict(X_test)

    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    
    # Log the metrics to separate files
    logging.info(f"MAE: {mae}")
    logging.info(f"MSE: {mse}")
    
    output_filepath = 'model.joblib'
    
    os.makedirs(output_path, exist_ok=True)
        
    # Save the trained model to disk using joblib
    joblib.dump(model, os.path.join(output_path,output_filepath))
    
    return True