from flask import Flask, jsonify, request
import joblib

# Assuming you have a trained model saved in 'model.joblib'

# Load the trained model
with open('./data/deploy/model.joblib', 'rb') as file:
    model = joblib.load(file)

# Create a Flask app
app = Flask(__name__)

# Define a route for your prediction endpoint
@app.route('/predict', methods=['GET'])
def predict():
    # Get the input values from the query parameters
    vol_moving_avg = float(request.args.get('vol_moving_avg'))
    adj_close_rolling_med = float(request.args.get('adj_close_rolling_med'))

    # Perform prediction using the loaded model
    prediction = model.predict([[vol_moving_avg, adj_close_rolling_med]])

    # Return the predicted volume as a JSON response
    response = {'prediction': prediction.tolist()}
    return jsonify(response)

# Run the Flask app
if __name__ == '__main__':
    app.run()
