"""
Module containing all functions needed to Preprocess the data:
    is_const_err(window_vals, limit)
    med_filter(window, med_window)
    range_check(window, UL, LL, all_vals)
"""
from statistics import median, mean
from numpy import nan
from pandas import isna

def is_null(datapoint):
    """
    Check if a DataPoint object contains null or invalid values for both the value and timestamp.

    Args:
        datapoint (DataPoint): The DataPoint object to be checked.

    Returns:
        bool: 
            - True if the value or timestamp is None, NaN, or invalid.
            - False if both the value and timestamp are valid.
    """
    #check values
    val = datapoint.value
    if val is None:
        return True
    elif val is nan or isna(val):
        return True
    
    #check times
    time = datapoint.time_stamp
    if time is None or time == '0':
        return True
    elif time is nan or isna(time):
        return True
    
    return False
    
    
def do_EMA(window, last_EMA, alpha):
    window_vals = window.get_win_vals()
    latest_val = window_vals[-1]
    
    #determine new EMA value
    new_EMA = alpha * latest_val + ((1 - alpha) * last_EMA)
    
    #update last reading to new EMA value
    window.change_val(-1, new_EMA)
    
    #update last EMA
    last_EMA = new_EMA
    return last_EMA


def med_filter(window, med_window):
    """
    Apply a median filter to the window values and update the window if necessary.

    Args:
        window (SlidingWindow): The sliding window object containing sensor readings.
        med_window (int): The size of the sub-window to apply the median filter to.

    Returns:
        None
    """
    window_vals = window.get_win_vals()
    med_arr = window_vals[:med_window]
    mid = med_window // 2

    if (med_arr[mid] != median(med_arr)):
        window.change_val(mid, median(med_arr))



def range_check(window, UL, LL, all_vals):
    """
    Check if values in the window are within a specified range and adjust if necessary.

    Args:
        window (SlidingWindow): The sliding window object containing sensor readings.
        UL (float): The upper limit for acceptable values.
        LL (float): The lower limit for acceptable values.
        all_vals (bool): Whether to check and adjust all values or only the latest one.

    Returns:
        None
    """
    window_vals = window.get_win_vals()
    if all_vals:
        for i in range(window.size):
            if (window_vals[i] > UL) or (window_vals[i] < LL):
                temp = window_vals[:i] + window_vals[i+1:]
                window.change_val(i, mean(temp))
    else:
        if (window_vals[-1] > UL) or (window_vals[-1] < LL):
            window_vals.pop()
            window.change_val(-1, mean(window_vals))
            


def main():
    window = [10,9,10,100,13,14,10]
    print(range_check(window, 75, -12, True))


if __name__ == "__main__":
    main()
