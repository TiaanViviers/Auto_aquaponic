import paho.mqtt.client as mqtt
import json
from dateutil import parser

class MQTTClient:
    """
    A client for interacting with an MQTT broker and receiving sensor data.

    Attributes:
        broker_address (str): The address of the MQTT broker.
        broker_port (int): The port of the MQTT broker.
        topic (str): The MQTT topic to subscribe to.
        client (mqtt.Client): The MQTT client instance.
        readings (list): A list to store received sensor data readings.
    """

    def __init__(self, broker_address, broker_port, topic):
        """
        Initialize the MQTTClient instance with broker details and topic.

        Args:
            broker_address (str): The address of the MQTT broker.
            broker_port (int): The port of the MQTT broker.
            topic (str): The MQTT topic to subscribe to.
        """
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.topic = topic
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.readings = []


    def on_connect(self, client, userdata, flags, rc):
        """
        Called when the client connects to the MQTT broker.

        Args:
            client (mqtt.Client): The MQTT client instance.
            userdata: The private user data.
            flags: Response flags sent by the broker.
            rc (int): The connection result code (0 for success)
        Returns:
            None
        """
        print(f"Connected successfully")
        client.subscribe(self.topic)


    def on_message(self, client, userdata, msg):
        try:
            # Decode the payload
            payload = json.loads(msg.payload.decode())
            
            # Extract relevant information
            value = float(payload['state'])
            data = payload.get('data', {})
            
            # Extract additional information from the 'data' field
            device_class = data.get('device_class', 'unknown')
            unit_of_measurement = data.get('unit_of_measurement', 'unknown')
            last_changed = data.get('last_changed', None)
            # Parse the last_changed timestamp if available
            if last_changed:
                timestamp = parser.isoparse(last_changed)
                human_time = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            else:
                timestamp = None
                human_time = None
            
            # Log the reading and metadata
            reading = (value, human_time, device_class, unit_of_measurement)
            # Store the reading
            self.readings.append(reading)

        except (ValueError, KeyError) as e:
            print(f"Failed to decode message: {e}")

    def connect(self):
        """
        Connect to the MQTT broker using the provided broker address and port.

        Returns:
            None
        """
        self.client.connect(self.broker_address, self.broker_port, 60)


    def start(self):
        """
        Start the MQTT client loop to listen for incoming messages.

        Returns:
            None
        """
        self.client.loop_start()


    def stop(self):
        """
        Stop the MQTT client loop and disconnect from the broker.

        Returns:
            None
        """
        self.client.loop_stop()
        self.client.disconnect()


    def get_readings(self):
        """
        Retrieve the list of sensor readings received from the MQTT broker.

        Returns:
            list: A list of tuples containing sensor readings and metadata.
        """
        return self.readings


    def clear_readings(self):
        """
        Clear the list of stored sensor readings.

        Returns:
            None
        """
        self.readings = []