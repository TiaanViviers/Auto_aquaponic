class DataPoint:
    """
    A class representing a single data point from a sensor.

    Attributes:
        value (float): The sensor reading value.
        time_stamp (str): The timestamp of when the reading was taken.
        sensor_type (str): The type of sensor that produced the reading.
        unit_of_measurement (str): The unit of measurement for the sensor reading.
    """

    def __init__(self, value, time_stamp, sensor_type, unit_of_measurement):
        """
        Initialize the DataPoint instance with sensor reading details.

        Args:
            value (float): The sensor reading value.
            time_stamp (str): The timestamp of when the reading was taken.
            sensor_type (str): The type of sensor that produced the reading.
            unit_of_measurement (str): The unit of measurement for the sensor reading.
        """
        self.value = value
        self.time_stamp = time_stamp
        self.sensor_type = sensor_type
        self.unit_of_measurement = unit_of_measurement

    def get_val(self):
        """
        Retrieve the sensor reading value.

        Returns:
            float: The sensor reading value.
        """
        return self.value
    
    def get_time(self):
        """
        Retrieve the timestamp of the sensor reading.

        Returns:
            str: The timestamp of when the reading was taken.
        """
        return self.time_stamp