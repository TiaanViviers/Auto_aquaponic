from mqtt_client import MQTTClient
from sliding_window import SlidingWindow
from data_point import DataPoint
from preprocessor import range_check, med_filter, do_EMA, is_null
from err_detections import is_const_err, CUSUM
from datetime import timedelta
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
        run_sensors(client, last_changed, last_EMA)
        
    elif sys.argv[1] == "csv":
        cleaned_data = run_csv(sys.argv[2], last_changed, last_EMA)
        datapoints_to_csv(cleaned_data, "INT_clean", True)
        print("new cleaned data csv made")
        return
    
    

################################ DYNAMIC SENSOR READINGS ################################

def run_sensors(client, last_changed, last_EMA):
    window = SlidingWindow(10)
    CT_plus_win = SlidingWindow(10)
    CT_min_win = SlidingWindow(10)
    clean_vals = []
    num_reads = 1
    
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
                no_err = CUSUM(CT_plus_win, CT_min_win, window.get_win_vals())
                if not no_err:
                    print("Drift detected in CUSUM")
                print(f"CT plus values: {CT_plus_win.as_list()}")
                print(f"CT minus values: {CT_min_win.as_list()}")
                
            client.clear_readings()
            time.sleep(1)

    except KeyboardInterrupt:
        # Stop the MQTT client correctly
        client.stop()


################################ STATIC CSV READINGS ################################

def run_csv(file_path, last_changed, last_EMA):
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
    
    while not window.is_full() and data_points:
        if not is_null(data_points[0]):
            window.add_reading(data_points.pop(0))
        else:
            data_points.pop(0)
        first_window = True
    
    while data_points:
        if first_window:
            last_changed[0] = window.get_win_vals()[-1]
            last_changed[1] = window.get_win_times()[-1]
            last_EMA = window.get_win_vals()[-1]

        #check for constant error sensor fault
        if is_const_err(window, last_changed, get_max_time(window)):
            print("CONSTANT ERROR DETECTED")

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
        no_err, target = CUSUM(CT_plus_win, CT_min_win, window.get_win_vals(), target)
        if not no_err:
            print(f"Drift detected in CUSUM at time: {window.as_list()[-1].time_stamp}")
        
        target_time = window.as_list()[-1].time_stamp
        write_csv(target, target_time, "INT_target")
        

        while len(data_points) > 0 and is_null(data_points[0]):
            data_points.pop(0)
        
        cleaned_val = window.slide_next(data_points.pop(0))
        cleaned_data.append(cleaned_val)
    
    cleaned_data += window.as_list()
    return cleaned_data


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