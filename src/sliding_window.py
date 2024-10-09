from collections import deque

class SlidingWindow:
    """
    A class representing a sliding window buffer that stores sensor readings.

    Attributes:
        size (int): The desired size of the sliding window.
        max_size (int): The maximum allowable size of the window buffer.
        window (deque): A deque object to hold sensor readings.
    """
    
    def __init__(self, size):
        """
        Initialize the SlidingWindow instance with the specified size.

        Args:
            size (int): The desired size of the sliding window.
        """
        self.size = size
        self.max_size = 40
        self.window = deque(maxlen=self.max_size)

    def add_reading(self, reading):
        """
        Add a new reading to the sliding window buffer.

        Args:
            reading (DataPoint): The new sensor reading to add.

        Returns:
            None
        """
        self.window.append(reading)
    
    
    def slide_next(self, reading):
        """
        Slide the window by adding a new reading and removing the oldest one.

        Args:
            reading (DataPoint): The new sensor reading to add.

        Returns:
            DataPoint: The oldest reading that was removed from the buffer.
        """
        self.window.append(reading)
        return self.window.popleft()


    def remove_idx(self, idx):
        """
        Remove a reading from the window by index.

        Args:
            idx (int): The index of the reading to remove.

        Returns:
            DataPoint: The removed reading.
        """
        buf_list = list(self.window)
        removed = buf_list.pop(idx)
        self.window = deque(buf_list, maxlen=self.max_size)
        return removed


    def get_win_vals(self):
        """
        Retrieve the values of all readings in the window.

        Returns:
            list: A list of sensor reading values.
        """
        obj_list = list(self.window)
        vals_list = []

        for i in range(len(self.window)):
            vals_list.append(obj_list[i].get_val())
        
        return vals_list
    

    def get_win_times(self):
        """
        Retrieve the timestamps of all readings in the window.

        Returns:
            list: A list of timestamps for the sensor readings.
        """
        obj_list = list(self.window)
        times_list = []

        for i in range(len(self.window)):
            times_list.append(obj_list[i].get_time())
        
        return times_list
    

    def get_sensor_type(self):
        """
        Get the sensor type from the first reading in the window.

        Returns:
            str: The sensor type of the first reading.
        """
        readings = list(self.window)
        return readings[0].sensor_type
    

    def change_val(self, index, new_val):
        """
        Modify the value of a specific reading in the window.

        Args:
            index (int): The index of the reading to modify.
            new_val (float): The new value to assign to the reading.

        Returns:
            None
        """
        readings = list(self.window)
        readings[index].value = new_val
        
    
    def as_list(self):
        """_summary_

        Returns:
            list: The window represented as a list
        """
        return list(self.window)
        

    def is_full(self):
        """
        Check if the sliding window buffer has reached its maximum size.

        Returns:
            bool: True if the buffer is full, False otherwise.
        """
        return len(self.window) >= self.size
    

def main():
    pass


if __name__ == '__main__' :
    main()