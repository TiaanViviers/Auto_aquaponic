from datetime import datetime
from numpy import std, mean

def is_const_err(window, last_changed, max_time):
    """
    Check if the sensor is experiencing a constant error, where the latest value
    matches the previously recorded value for a time exceeding the allowed maximum.

    Args:
        window (SlidingWindow): The sliding window object containing sensor readings.
        last_changed (list): A list containing the last recorded value and its timestamp
                             in the format [value, timestamp].
        max_time (timedelta): The maximum allowed time between value changes before
                              it is considered a constant error.

    Returns:
        bool: True if a constant error is detected (value unchanged for too long), 
              False otherwise.
    """
    window_times = window.get_win_times()
    window_vals = window.get_win_vals()
    
    if window_vals[-1] == last_changed[0]:
        #new value == last changed value
        time_diff = time_difference(last_changed[1], window_times[-1])
        if time_diff > max_time:
            return True
        
    else:
        #new value != last changed value
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


def CUSUM(dev_plus_win, dev_minus_win, window_vals, last_smoothed):
    """
    Compute the next values for the CUSUM control chart using a window of deviations.

    Args:
        dev_plus_win (SlidingWindow): A sliding window holding recent positive deviations.
        dev_minus_win (SlidingWindow): A sliding window holding recent negative deviations.
        window_vals (list): The current window of data points.

    Returns:
        boolean: A boolean indicating if the process is in control.
    """
    target = get_target(window_vals, last_smoothed)
    slack = get_slack(window_vals)
    control_lim = get_control_lim(window_vals)
    
    xi = window_vals[-1]  # Latest data point

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

    # Check control limits
    if CT_plus_sum > control_lim or CT_minus_sum > control_lim:
        return False, target, control_lim # Process is out of control
    else:
        return True, target, control_lim # Process is in control



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

def get_target(window_vals, last_smoothed):
    #return mean(window_vals)
    return AES_target(window_vals, last_smoothed)
    
    
def mean_target(window_vals, last_smoothed=0):
    """
    Calculate the target value as the mean of the window values.

    Args:
        window_vals (list): A list of sensor readings.

    Returns:
        float: The target value.
    """
    return mean(window_vals)
    
    

def AES_target(window_vals, last_smoothed):
    alpha = get_alpha(window_vals)
    xt = window_vals[-1]
    target = alpha * xt + (1-alpha)*last_smoothed
    return target
    
    
def get_alpha(window_vals):
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
    
    