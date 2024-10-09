import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import numpy as np
import pandas as pd
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler

class Model:
    """
    A class used to represent and interact with sensor models for making predictions.
    
    Attributes:
        sensor_type (str): The type of sensor, which determines the model used (e.g., 'temp').
        model (keras.Model): The loaded machine learning model specific to the sensor type.
        scaler (MinMaxScaler): A scaler for normalizing the input data.
    """
    def __init__(self, sensor_type):
        """
        Initializes the Model class based on the sensor type.
        
        Args:
            sensor_type (str): The type of sensor (e.g., 'temp', 'humidity'). 
                               Determines which model to load.
        
        Raises:
            ValueError: If the sensor type is not supported.
        """
        self.sensor_type = sensor_type
        self.model = None
        self.scaler = MinMaxScaler()

        # Load the appropriate model based on the sensor type
        if sensor_type == 'temp':
            self.model = load_model('../models/temp_model.keras')
        else:
            raise ValueError(f"Model for sensor type '{sensor_type}' is not available yet.")
        
    
    def preprocess_temp_data(self, last_values, last_times):
        """
        Preprocesses temperature data by scaling values and generating cyclical 
        time features.
        
        Args:
            last_values (list of float): Last 10 temperature readings.
            last_times (list of pd.Timestamp): Corresponding timestamps for the 
                                               last 10 readings.
        
        Returns:
            np.array: Preprocessed input for the temperature model, reshaped to (1, 10, 3).
        """
        # Convert the list of times to a DataFrame and create cyclical time features
        df = pd.DataFrame({'Time': last_times, 'State': last_values})
        df['Time'] = pd.to_datetime(df['Time'])
        df['Hour'] = df['Time'].dt.hour

        # Create cyclical features for the hour
        df['Hour_sin'] = np.sin(2 * np.pi * df['Hour'] / 24)
        df['Hour_cos'] = np.cos(2 * np.pi * df['Hour'] / 24)
        
        # Scale the temperature values (State)
        df['State'] = self.scaler.fit_transform(df[['State']])
        
        # Return only the last 10 preprocessed values as input
        return df[['State', 'Hour_sin', 'Hour_cos']].values.reshape(1, 10, 3)
    
    
    def predict(self, last_values, last_times):
        """
        Predicts future sensor values based on the last 10 readings using the trained model.
        
        Args:
            last_values (list of float): Last 10 readings from the sensor.
            last_times (list of pd.Timestamp): Corresponding timestamps for the
                                               last 10 readings.
        
        Returns:
            tuple: 
                predicted_values (list of float): The predicted sensor values 
                                                  (48 future predictions).
                future_timestamps (list of pd.Timestamp): Corresponding future timestamps 
                        for the predicted values.
        
        Raises:
            ValueError: If the sensor type is not supported.
        """
        if self.sensor_type == 'temp':
            # Preprocess data specifically for temperature
            preprocessed_input = self.preprocess_temp_data(last_values, last_times)

            # Predict 48 future values
            predictions = self.model.predict(preprocessed_input)

            # Inverse transform the predictions to original scale
            predictions = self.scaler.inverse_transform(predictions).flatten()

            # Convert last_time to pd.Timestamp if it's not already
            last_time = pd.to_datetime(last_times[-1])  # Ensure it's a Timestamp object

            # Create future timestamps based on the last timestamp (e.g., 25-second intervals)
            future_timestamps = [last_time + pd.Timedelta(seconds=25 * (i + 1)) for i in range(48)]

            return predictions, future_timestamps
        else:
            raise ValueError(f"sensor type '{self.sensor_type}' is not implemented yet.")


def test_client():
    """
    A test client that creates a Model instance, passes temperature values,
    and generates predictions.
    
    Generates 10 temperature values and timestamps, and then uses the Model
    to predict 48 future values.
    """
    model = Model('temp')
    vals = [22.1, 22.4, 23, 22.9, 22.5, 22.6, 23, 23.2, 23.7, 23.6]

    # Generate 10 time values, 25 seconds apart, starting from the current time
    start_time = pd.Timestamp.now()
    time_values = pd.date_range(start=start_time, periods=10, freq='25s')

    # Pass the values and time_values to the model for predictions
    predicted_values, future_timestamps = model.predict(vals, time_values)

    print("Predicted Values:", predicted_values)
    print("Future Timestamps:", future_timestamps)
    
if __name__ == "__main__":
    test_client()