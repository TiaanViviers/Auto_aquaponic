from mqtt_client import MQTTClient
from sliding_window import SlidingWindow
from telebot import Telebot
from data_point import DataPoint
from preprocessor import range_check, med_filter, do_EMA, is_null
from err_detections import is_const_err, CUSUM
from format_time import time_formatter
from datetime import timedelta
from prediction import Model
from dotenv import load_dotenv
import os
import sys
import time
import csv
import pandas as pd

def main():
    """
    Main function that initiates the MQTT client or CSV processing based on command-line arguments.
    
    For "mqtt" DYNAMIC mode:
        Connects to the MQTT broker, retrieves readings, processes them, and logs to CSV.

    For "csv" STATIC mode:
        Cleans data from the specified CSV file and writes it back to a new cleaned CSV file.

    Args:
        None (but uses command-line arguments for "mqtt" or "csv" mode).

    Returns:
        None
    """
    last_changed = [0, 0]  #value, timestamp
    last_EMA = 0
    
    if (len(sys.argv) not in (2, 3)) or (sys.argv[1] not in ("params", "mqtt", "csv")):
        print("Invalid program arguments")
        print("Run <python/python3 main.py params> to see parameter options")
        return
        
    
    if sys.argv[1] == "params":
        print("For Dynamic sensor readings:")
        print("     argv 1 = mqtt")
        print("         argv 2 = pvolt | bvolt | temp | illum | ph | humid")
        print()
        print("For Static data from csv file:")
        print("     argv 1 = csv")
        print("         argv 2 = csv file path eg ../Data/<csv_filename.csv>")
        return
    
    elif sys.argv[1] == "mqtt":
        #load environment variables
        load_dotenv()
        # Set up and start the MQTT client
        broker_address = os.getenv("MQTT_BROKER")
        broker_port = int(os.getenv("MQTT_PORT"))
        topic = os.getenv(sys.argv[2])
        mqtt_client = MQTTClient(broker_address, broker_port, topic)
        mqtt_client.connect()
        mqtt_client.start()
        client = mqtt_client
        # Set up telegram Bot
        BOT_TOKEN = os.getenv("BOT_TOKEN")
        OPS_CHAT_ID = os.getenv("OPS_CHAT_ID")
        TECH_CHAT_ID = os.getenv("TECH_CHAT_ID")
        bot = Telebot(BOT_TOKEN, OPS_CHAT_ID, TECH_CHAT_ID)
        model = Model(sys.argv[2])
        run_sensors(client, last_changed, last_EMA, bot, model)
        
    elif sys.argv[1] == "csv":
        #load environment variables
        load_dotenv()
        # Set up telegram Bot
        BOT_TOKEN = os.getenv("BOT_TOKEN")
        OPS_CHAT_ID = os.getenv("OPS_CHAT_ID")
        TECH_CHAT_ID = os.getenv("TECH_CHAT_ID")
        bot = Telebot(BOT_TOKEN, OPS_CHAT_ID, TECH_CHAT_ID)
        
        #format inputfile
        time_formatter(sys.argv[2])
        #run inputfile
        cleaned_data = run_csv(sys.argv[2], last_changed, last_EMA, bot)
        datapoints_to_csv(cleaned_data, "clean", True)
        print("new cleaned data csv made")
    

################################ DYNAMIC SENSOR READINGS ################################

def run_sensors(client, last_changed, last_EMA, bot, model):
    """
    Continuously monitors sensor readings, processes them, and performs error 
    detection, filtering, and prediction using the specified model.

    Args:
        client (MQTTClient): The client that retrieves sensor readings.
        last_changed (list): A list containing the last value and time that changed
                             from the sensor.
        last_EMA (float): The Exponential Moving Average (EMA) of previous 
                          sensor readings.
        bot (TelegramBot): A bot instance to send alerts/error messages (via Telegram).
        model (Model): The prediction model used to detect dangerous conditions
                       based on sensor readings.

    Raises:
        KeyboardInterrupt: Stops the process when interrupted by the user.

    The function performs the following steps:
        - Retrieves sensor readings from the client in a loop.
        - Checks for null values and logs readings to a CSV file.
        - Stores the readings in a sliding window for preprocessing.
        - Applies preprocessing techniques like Exponential Moving Average ,
          range checks, and filtering.
        - Detects constant error sensor faults and logs errors if found.
        - Runs CUSUM (Cumulative Sum) control for drift detection and logs the results.
        - Uses the model to predict potentially dangerous future values and
          logs any dangerous predictions.
    
    Loops indefinitely, processing each new reading, until interrupted by the user.
    """
    window = SlidingWindow(10)
    CT_plus_win = SlidingWindow(10)
    CT_min_win = SlidingWindow(10)
    clean_vals = []
    num_reads = 1
    model = None
    
    try:
        while True:
            ################### retrieve and format new reading ###################
            reading = None
            readings_list = client.get_readings()
            if readings_list:
                raw_reading = readings_list[0]
                print(f"Reading number {num_reads} is: {raw_reading}")
                reading = DataPoint(raw_reading[0], raw_reading[1], raw_reading[2], raw_reading[3])
                
                #check for Null values
                if is_null(reading):
                    reading = None
                    num_reads -= 1
                
                if num_reads == 1:
                    datapoints_to_csv([reading], "raw", True)
                    last_EMA = reading.value
                    model = Model(get_model_type(reading.sensor_type))
                else: 
                    datapoints_to_csv([reading], "raw", False)
                num_reads += 1
                
            if reading and window.is_full():
                clean_vals.append(window.slide_next(reading))
                
            elif reading:
                window.add_reading(reading)
                
            
            ###################### Preprocess new readings #####################
            if window.is_full() and reading:
                
                if num_reads == 10:
                    last_changed[0] = window.get_win_vals()[-1]
                    last_changed[1] = window.get_win_times()[-1]
            
                #check for constant error sensor fault
                if is_const_err(window, last_changed, get_max_time(window)):
                    err_log(bot, 1, window.as_list()[-1])
                    print("CONSTANT ERROR DETECTED")

                #perform range check on current window
                if num_reads == 10:
                    range_check(window, get_UL(window), get_LL(window), True)
                else:
                    range_check(window, get_UL(window), get_LL(window), False)
                    
                #perform EMA smoothing
                last_EMA = do_EMA(window, last_EMA, 0.4)

                #perform median filtering to smooth data
                med_filter(window, 3)
                
            ######################### Error Detection ##########################
                no_err = CUSUM(CT_plus_win, CT_min_win, window.get_win_vals(), last_EMA)
                if not no_err:
                    print("Drift detected in CUSUM")
                print(f"CT plus values: {CT_plus_win.as_list()}")
                print(f"CT minus values: {CT_min_win.as_list()}")

                
            ######################### Model Prediction ##########################
                if is_dangerous_prediction(model, window, bot):
                    print("DANGEROUS PREDICTION DETECTED")
                
            client.clear_readings()
            time.sleep(1)

    except KeyboardInterrupt:
        # Stop the MQTT client correctly
        client.stop()


################################ STATIC CSV READINGS ################################

def run_csv(file_path, last_changed, last_EMA, bot):
    """
    Clean data from a CSV file by processing sensor readings and applying filters.

    Args:
        file_path (str): The path to the CSV file containing raw data.

    Returns:
        list: A list of cleaned DataPoint objects.
    """
    data_points = csv_to_datapoints(file_path)
    cleaned_data = []
    window = SlidingWindow(10)
    CT_plus_win = SlidingWindow(10)
    CT_min_win = SlidingWindow(10)
    target = data_points[0].value
    model = None
    
    while not window.is_full() and data_points:
        if not is_null(data_points[0]):
            window.add_reading(data_points.pop(0))
        else:
            data_points.pop(0)
        first_window = True
        
    model = Model(get_model_type(window.get_sensor_type()))
    
    while data_points:
        if first_window:
            last_changed[0] = window.get_win_vals()[-1]
            last_changed[1] = window.get_win_times()[-1]
            last_EMA = window.get_win_vals()[-1]

        #check for constant error sensor fault
        if is_const_err(window, last_changed, get_max_time(window)):
            err_log(bot, 1, window.as_list()[-1])
            print("CONSTANT VALUE ERR DETECTED")

        #perform range check on current window
        if first_window:
            first_window = False
            range_check(window, get_UL(window), get_LL(window), True)
        else:
            range_check(window, get_UL(window), get_LL(window), False)
            
        #perform median filtering to smooth data
        med_filter(window, 3)
            
        #perform EMA smoothing
        last_EMA = do_EMA(window, last_EMA, 0.4)
        
        #error detection with CUSUM
        no_err, target, cl = CUSUM(CT_plus_win, CT_min_win, window.get_win_vals(), target)
        if not no_err:
            err_log(bot, 2, window.as_list()[-1], cl=cl)
            print(f"Drift detected in CUSUM at time: {window.as_list()[-1].time_stamp}")
        
        #target_time = window.as_list()[-1].time_stamp
        #write_csv(target, target_time, "target")
        
        #Prediction error check
        if is_dangerous_prediction(model, window, bot):
            print("DANGEROUS PREDICTION DETECTED")
        

        while len(data_points) > 0 and is_null(data_points[0]):
            data_points.pop(0)
        
        cleaned_val = window.slide_next(data_points.pop(0))
        cleaned_data.append(cleaned_val)
    
    cleaned_data += window.as_list()
    return cleaned_data


############################## TELEGRAM COMMUNICATION #############################
def err_log(bot, type, datapoint, cl=None, pred_time=None):
    """
    Logs errors and alerts using a bot, optionally including prediction and control
    limit information.

    Args:
        bot (TelegramBot): The bot instance responsible for logging messages or
        sending alerts (e.g., via Telegram).
        type (int): The type of error or alert to log (refer to telebot docs for types).
        datapoint (DataPoint): The sensor reading data point that triggered the error
                               or alert.
        cl (float, optional): The control limit (if applicable). Defaults to None.
        pred_time (datetime, optional): The timestamp of the model's prediction 
                                        (if applicable). Defaults to None.

    The function logs:
        - The value, timestamp, sensor type, and unit from the `datapoint`.
        - The control limit (`cl`) if provided.
        - The predicted time (`pred_time`) and corresponding upper (`up`) and lower
          (`low`) ranges if `pred_time` is provided.

    If `pred_time` is provided, the function also calculates the upper and lower 
    prediction range limits using `get_ML_range`.

    """
    val = round(datapoint.value, 2)
    time = datapoint.time_stamp
    sensor = datapoint.sensor_type
    unit = datapoint.unit_of_measurement
    cl = round(cl, 2) if cl else None
    
    if  not pred_time:
        pred_time = None; up = None; low = None
    else:
        up, low = get_ML_range(sensor)
    
    bot.log(type, val, time, sensor, unit, cl, pred_time=pred_time, range_up=up, range_low=low)
    

############################### PREDICTIVE MODELLING ###############################

def get_model_type(sensor_type):
    """
    Maps sensor types to their corresponding model types.

    Args:
        sensor_type (str): The type of the sensor (e.g., 'SSTEMP_sensor')

    Returns:
        str: The model type associated with the given sensor type. Possible return values:
            - 'pvolt' for 'Pvoltage_sensor'.
            - 'bvolt' for 'Bvoltage_sensor'.
            - 'temp' for temperature sensors ('SSTEMP_sensor' and 'TEMP_sensor').
            - 'illum' for 'illuminance_sensor'.
            - 'hum' for 'SSHUM_sensor'.
            - 'ph' for 'PH_sensor'.
            - None for 'WINDDIR_sensor' (currently not handled).

    Raises:
        None: Function will return `None` if an unsupported sensor type is passed.
    """
    if sensor_type == "Pvoltage_sensor":
        return 'pvolt'
    if sensor_type == "Bvoltage_sensor":
        return 'bvolt'
    if sensor_type in ('SSTEMP_sensor', 'TEMP_sensor'):
        return 'temp'
    if sensor_type == 'illuminance_sensor':
        return 'illum'
    if sensor_type == 'SSHUM_sensor':
        return 'hum'
    if sensor_type == 'PH_sensor':
        return 'ph'
    if sensor_type == 'WINDDIR_sensor':
        pass


def is_dangerous_prediction(model, window, bot):
    """
    Checks if the model's prediction falls outside the safe range and logs an alert if necessary.

    Args:
        model (Model): The model used to predict future sensor values.
        window (SlidingWindow): A sliding window object containing recent 
                                sensor readings and their timestamps.
        bot (TelegramBot): The bot instance used for logging and sending alerts.

    Returns:
        bool: Returns `True` if the prediction is considered dangerous 
              (i.e., outside the safe range), `False` otherwise.

    Functionality:
        - Retrieves the last set of sensor values and their corresponding 
          timestamps from the sliding window.
        - Uses the model to predict future sensor values based on the recent window data.
        - Retrieves the safe range (upper and lower limits) for the sensor type.
        - If the model's last predicted value falls outside this range, logs the 
          event using `err_log` and returns `True`.
        - Otherwise, returns `False` to indicate the prediction is within the safe range.
    """
    #get prediction
    vals = window.get_win_vals()
    times = window.get_win_times()
    predicted_values, timestamps = model.predict(vals, times)
    
    #get sensor ranges
    sensor = window.get_sensor_type()
    upper, lower = get_ML_range(sensor)
    
    if int(predicted_values[-1]) not in range(lower, upper):
        unit = window.as_list()[0].unit_of_measurement
        datapoint = DataPoint(int(predicted_values[-1]), times[-1], sensor, unit)
        err_log(bot, 3, datapoint, pred_time=timestamps[-1])
        return True
    
    return False
        

################################ UTILITY FUNCTIONS ################################

def get_UL(window):
    """
    Get the upper limit (UL) for a sensor based on its type.

    Args:
        window (SlidingWindow): The sliding window object containing sensor readings.

    Returns:
        float: The upper limit for the specified sensor type.
    """
    sensor_type = window.get_sensor_type()
    if sensor_type == "Pvoltage_sensor":
        UL = 50
    
    if sensor_type == "Bvoltage_sensor":
        UL = 50
        
    if sensor_type in ('SSTEMP_sensor', 'TEMP_sensor'):
        UL = 45
        
    if sensor_type == 'illuminance_sensor':
        UL = 130_000
        
    if sensor_type == 'SSHUM_sensor':
        UL = 85
        
    if sensor_type == 'PH_sensor':
        UL = 50
    
    if sensor_type == 'WINDDIR_sensor':
        UL = 360
    
    return UL


def get_LL(window):
    """
    Get the lower limit (LL) for a sensor based on its type.

    Args:
        window (SlidingWindow): The sliding window object containing sensor readings.

    Returns:
        float: The lower limit for the specified sensor type.
    """
    sensor_type = window.get_sensor_type()
    if sensor_type == "Pvoltage_sensor":
        LL = -0.00000001
    
    if sensor_type == "Bvoltage_sensor":
        LL = -0.00000001
        
    if sensor_type in ('SSTEMP_sensor', 'TEMP_sensor'):
        LL = -10
    
    if sensor_type == 'illuminance_sensor':
        LL = -0.00000001
        
    if sensor_type == 'SSHUM_sensor':
        LL = 10
        
    if sensor_type == 'PH_sensor':
        LL = -20
        
    if sensor_type == 'WINDDIR_sensor':
        LL = 0

    return LL


def get_max_time(window):
    """
    Retrieve the maximum allowed time for readings based on the sensor type.

    Args:
        window (SlidingWindow): The sliding window object containing sensor readings.

    Returns:
        timedelta: The maximum time allowed for readings from the sensor.
    """
    sensor_type = window.get_sensor_type()
    max_time = timedelta(minutes=30)
    
    if sensor_type == "Pvoltage_sensor":
        max_time = timedelta(minutes=30)
    
    if sensor_type == "Bvoltage_sensor":
        max_time = timedelta(minutes=30)
        
    if sensor_type in ('SSTEMP_sensor', 'TEMP_sensor'):
        max_time = timedelta(minutes=30)
    
    if sensor_type == 'illuminance_sensor':
        max_time = timedelta(minutes=30)
        
    if sensor_type == 'SSHUM_sensor':
        max_time = timedelta(minutes=30)
    
    if sensor_type == 'PH_sensor':
        max_time = timedelta(minutes=30)
        
    if sensor_type == 'WINDDIR_sensor':
        max_time = timedelta(minutes=30)
    
    return max_time


def get_ML_range(sensor_type):
    """
    Returns the acceptable range (lower and upper limits) for the given sensor type.

    Args:
        sensor_type (str): The type of sensor for which the acceptable range is requested.

    Returns:
        tuple: A tuple containing:
            - high (float): The upper limit of the acceptable range for the sensor type.
            - low (float): The lower limit of the acceptable range for the sensor type.
    """
    if sensor_type == "Pvoltage_sensor":
        low = -0.000001; high = 50
    
    if sensor_type == "Bvoltage_sensor":
        low = -0.000001; high = 50
        
    if sensor_type in ('SSTEMP_sensor', 'TEMP_sensor'):
        low = 8; high = 30
    
    if sensor_type == 'illuminance_sensor':
        low = -0.000001; high = 100_000
        
    if sensor_type == 'SSHUM_sensor':
        low = 15; high = 80
    
    if sensor_type == 'PH_sensor':
        low = -15; high = 50
        
    if sensor_type == 'WINDDIR_sensor':
        low = -0.000001; high = 361
    
    return high, low


################################ READING / WRITING ################################

def csv_to_datapoints(file_path):
    """
    Convert data from a CSV file into a list of DataPoint objects.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        list: A list of DataPoint objects created from the CSV rows.
    """
    data_points = []

    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)

        # Skip the header if present
        next(csv_reader, None)

        # Iterate through each row in the CSV
        for row in csv_reader:
            # Extract data from each row
            if row[0] == '':
                break
            state = float(row[0])  
            time = row[1]
            device = row[2]
            unit = row[3]

            # Create a DataPoint object from the CSV row
            data_point = DataPoint(state, time, device, unit)

            # Append the DataPoint object to the list
            data_points.append(data_point)

    return data_points


def datapoints_to_csv(data_points, file_type, write_all):
    """
    Write a list of DataPoint objects to a CSV file.

    Args:
        data_points (list): The list of DataPoint objects to write.
        file_type (str): The type of file being written ("raw" or "clean").
        write_all (bool): Whether to overwrite the file (True) or append to it (False).

    Returns:
        None
    """
    file_path = "../data/" + file_type + "_" + data_points[0].sensor_type + ".csv"
    
    mode = 'w' if write_all else 'a'
    
    with open(file_path, mode=mode, newline='') as file:
        csv_writer = csv.writer(file)

        # Write the header
        if write_all:
            csv_writer.writerow(['State', 'Time', 'Device', 'Unit'])

        # Iterate through the DataPoint objects and write each to a row
        for data_point in data_points:
            # Create a row from the DataPoint attributes
            row = [
                data_point.get_val(),  # state
                data_point.get_time(),  # time
                data_point.sensor_type,  # device
                data_point.unit_of_measurement  # unit
            ]

            # Write the row to the CSV file
            csv_writer.writerow(row)


def write_csv(value, timestamp, output_path):
    """
    Writes a value and timestamp to a CSV file. If the file already exists, it
    appends the new data; otherwise, it creates a new file with the header.
 
    Args:
        value (float): The target value to be written to the CSV file.
        timestamp (str or pd.Timestamp): The timestamp associated with the value.
        output_path (str): The name of the CSV file (without the ".csv" extension)
                           where the data will be stored.The file will be created
                           or appended in the "../data/" directory.
    
    Raises:
        None: The function does not raise any errors but will fail silently if the directory "../data/" does not exist.
    """
    output_path = "../data/" + output_path + ".csv"
    # Create a DataFrame with the given value and timestamp
    data = {'Target': [value], 'Time': [timestamp]}
    df = pd.DataFrame(data)

    # Check if the file already exists
    if not os.path.exists(output_path):
        # File doesn't exist, write with header
        df.to_csv(output_path, mode='w', header=True, index=False)
    else:
        # File exists, append without writing the header
        df.to_csv(output_path, mode='a', header=False, index=False)
            
            
if __name__ == '__main__' :
    main()