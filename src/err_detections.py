from datetime import datetime
from numpy import std
import pandas as pd
import os

def is_const_err(window, last_changed, max_time):
    """
    Check if the sensor is experiencing a constant error, where the latest target
    matches the previously recorded target for a time exceeding the allowed maximum.

    Args:
        window (SlidingWindow): The sliding window object containing sensor readings.
        last_changed (list): A list containing the last recorded target and its timestamp
                             in the format [target, timestamp].
        max_time (timedelta): The maximum allowed time between target changes before
                              it is considered a constant error.

    Returns:
        bool: True if a constant error is detected (target unchanged for too long), 
              False otherwise.
    """
    window_times = window.get_win_times()
    window_vals = window.get_win_vals()
    
    if window_vals[-1] == last_changed[0]:
        #new target == last changed target
        time_diff = time_difference(last_changed[1], window_times[-1])
        if time_diff > max_time:
            return True
        
    else:
        #new target != last changed target
        last_changed[0] = window_vals[-1]
        last_changed[1] = window_times[-1]
    
    return False


def time_difference(time1, time2):
    """
    Calculate the difference between two timestamps in the format "%H:%M:%S".

    Args:
        time1 (str): The first timestamp as a string.
        time2 (str): The second timestamp as a string.

    Returns:
        timedelta: The difference between the two times as a timedelta object.
    """
    time_format = "%Y-%m-%d %H:%M:%S"

    t1 = datetime.strptime(time1, time_format)
    t2 = datetime.strptime(time2, time_format)
    
    # Calculate the difference (this will be a timedelta object)
    time_diff = t2 - t1
    
    return time_diff


def CUSUM(dev_plus_win, dev_minus_win, window, last_smoothed):
    """
    Compute the next targets for the CUSUM control chart using a window of deviations.

    Args:
        dev_plus_win (SlidingWindow): A sliding window holding recent positive deviations.
        dev_minus_win (SlidingWindow): A sliding window holding recent negative deviations.
        window (list): The current window of data points.

    Returns:
        boolean: A boolean indicating if the process is in control.
    """
    window_vals = window.get_win_vals()
    target = get_AES_target(window_vals, last_smoothed)
    slack = get_slack(window_vals)
    control_lim = get_control_lim(window_vals)
    
    xi = window_vals[-1]

    # Calculate the deviation from the target
    dev = xi - target

    # Calculate deviations for CUSUM
    dev_plus = dev - slack     #positive dev beyond slack
    dev_minus = -dev - slack   #negative dev beyond slack

    # Ensure deviations are non-negative
    dev_plus = max(0, dev_plus)
    dev_minus = max(0, dev_minus)

    # Add new deviations to the windows
    if dev_plus_win.is_full():
        dev_plus_win.slide_next(float(dev_plus))
        dev_minus_win.slide_next(float(dev_minus))
    else:
        dev_plus_win.add_reading(float(dev_plus))
        dev_minus_win.add_reading(float(dev_minus))
        

    # Calculate cumulative sums over the deviation windows
    CT_plus_sum = sum(dev_plus_win.as_list())
    CT_minus_sum = sum(dev_minus_win.as_list())

    #write target + slack to csv
    write_csv(target, slack, window.get_win_times()[-1], "Valid_target")

    # Check control limits
    if CT_plus_sum > control_lim or CT_minus_sum > control_lim:
        return False, control_lim # Process is out of control
    else:
        return True, control_lim # Process is in control



def get_slack(sensor_readings, k=0.5):
    
    """
    Calculate slack based on the standard deviation of sensor readings.
    
    Args:
        sensor_readings (list): A list of sensor readings in the sliding window.
        k (float): Multiplier to adjust the slack level. Default is 0.5.
        
    Returns:
        float: The calculated slack based on the standard deviation.
    """
    return k * std(sensor_readings)

def get_control_lim(sensor_readings, k=5):
    """
    Calculate the control limit based on the standard deviation of sensor readings.

    Args:
        sensor_readings (list): A list of sensor readings in the sliding window.
        h (float): Multiplier for the control limit.

    Returns:
        float: The control limit.
    """
    
    return k * std(sensor_readings)
      
def get_AES_target(window_vals, last_smoothed):
    """
    Calculates the Adaptive Exponential Smoothing (AES) target based on the current 
    window of targets and the last smoothed target.
    
    Args:
        window_vals (list of float): A list of the most recent sensor readings.
        last_smoothed (float): The last smoothed target using the AES method.
    
    Returns:
        float: The next smoothed target target, calculated as an exponential moving average.
    
    Functionality:
        - The function first calculates the smoothing factor `alpha` using `get_alpha()`, 
          which adapts based on the standard deviation of the targets in the window.
        - The last target in the window (`xt`) is combined with the previous smoothed target 
          using `alpha` to generate the next smoothed target (`target`).
    """
    alpha = get_alpha(window_vals)
    xt = window_vals[-1]
    target = alpha * xt + (1-alpha)*last_smoothed
    return target
      
def get_alpha(window_vals):
    """
    Calculates the smoothing factor `alpha` based on the variability 
    of the window of targets.
    
    Args:
        window_vals (list of float): A list of the most recent sensor readings (window of targets).
    
    Returns:
        float: The smoothing factor `alpha`, constrained between 0 and 1, 
               which reflects the variability in the window.
    
    Functionality:
        - The function calculates the normalized standard deviation (`std_norm`)
          of the window targets.
        - It computes the maximum possible standard deviation (`std_max`) as 
          half the range of the targets.
        - The smoothing factor `alpha` is the ratio of `std_norm` to `std_max`. 
          If there is no variability (`std_max == 0`), `alpha` is set to 0.
        - Ensures that `alpha` is bounded between 0 and 1, meaning no smoothing 
          (alpha = 1) or full smoothing (alpha = 0).
    """
    std_norm = std(window_vals)
    x_min = min(window_vals)
    x_max = max(window_vals)
    delta_x = x_max - x_min

    # Calculate std_max as half the range
    std_max = delta_x / 2.0

    # Handle division by zero
    if std_max == 0:
        alpha = 0.0  # No variability in the window
    else:
        alpha = std_norm / std_max
        # Ensure alpha is between 0 and 1
        alpha = max(0, min(1, alpha))
    
    return alpha

def write_csv(target, slack, timestamp, output_path):
    """
    Writes a target and timestamp to a CSV file. If the file already exists, it
    appends the new data; otherwise, it creates a new file with the header.
 
    Args:
        target (float): The target value to be written to the CSV file.
        slack (float): The slack value used to calculate upper and lower bounds.
        timestamp (str or pd.Timestamp): The timestamp associated with the target.
        output_path (str): The name of the CSV file (without the ".csv" extension)
                           where the data will be stored.The file will be created
                           or appended in the "../data/" directory.
    
    Raises:
        None: The function does not raise any errors but will fail silently if the directory
        "../data/" does not exist.
    """
    output_path = "../data/" + output_path + ".csv"
    ub = target + slack
    lb = target - slack
    # Create a DataFrame with the given target and timestamp
    data = {'Target': [target], 'Time': [timestamp], 'Upper_Bound': [ub], 'Lower_Bound': [lb]}
    df = pd.DataFrame(data)

    try:
        if not os.path.exists(output_path):
            # File doesn't exist, write with header
            df.to_csv(output_path, mode='w', header=True, index=False)
        else:
            # File exists, append without header
            df.to_csv(output_path, mode='a', header=False, index=False)
    except Exception as e:
        print(f"An error occurred while writing to the CSV: {e}")