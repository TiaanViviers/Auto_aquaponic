import requests

class Telebot:
    """
    A class to interact with Telegram Bot API to send messages for logging sensor errors.

    Attributes:
        token (str): The token used to authenticate with the Telegram Bot API.
        ops_chat_id (str): The chat ID for operations-related alerts.
        tech_chat_id (str): The chat ID for technical-related alerts.
    """
    
    def __init__(self, token, ops_chat_id, tech_chat_id):
        """
        Initializes the Telebot class with authentication token and chat IDs.
        
        Args:
            token (str): The token for the Telegram Bot API.
            ops_chat_id (str): The chat ID for operational alerts.
            tech_chat_id (str): The chat ID for technical alerts.
        """
        self.token = token
        self.ops_chat_id = ops_chat_id
        self.tech_chat_id = tech_chat_id

    def log(self, error_type, value, time, sensor, unit, cl=None, pred_time=None,
                  range_up=None, range_low=None):
        """
        Logs error messages based on sensor readings and sends them via Telegram.
        
        Args:
            error_type (int): The type of error (1 for constant error, 2 for CUSUM drift,
                              3 for prediction error).
            value (float): The sensor value that triggered the error.
            time (str): The timestamp when the error occurred.
            sensor (str): The type of sensor reporting the error.
            unit (str): The unit of measurement for the sensor value.
            cl (float, optional): Control limit, if applicable. Defaults to None.
            pred_time (str, optional): The predicted timestamp for prediction errors.
            range_up (float, optional): The upper limit of the predicted range for
                                        the sensor.
            range_low (float, optional): The lower limit of the predicted range for
                                        the sensor.
        
        Functionality:
            - Based on the `error_type`, formats a message specific to the type of error:
              1. Constant Value Error.
              2. CUSUM Drift Error.
              3. Prediction Error.
            - Sends the message to the appropriate chat channel 
              ('ops' for operational alerts, 'tech' for technical alerts).
        """
        message = None

        if error_type == 1:
            # Stuck at zero
            if value == 0.00:
                message = (f"Detected 'Stuck at Zero' error for sensor {sensor} at"
                           f" time {time}.")
            else:
                # Constant Value Error
                message = (f"Detected 'Constant Value Error' for sensor {sensor} at "
                           f"value {value}{unit} @ {time}.")
            self.send_message(message, 'tech')

        elif error_type == 2:
            # Cusum error
            message = (f"Sensor {sensor} has drifted past the control limit or the "
                       f"precision has degraded. It is above the Control Limit ({cl}{unit}) "
                       f"with value {value}{unit} at time {time}.")
            self.send_message(message, 'tech')

        elif error_type == 3:
            # Prediction error
            message = (f"Sensor {sensor} is predicted to be outside of the optimal "
                       f"range ({range_low}{unit}, {range_up}{unit}) with the value "
                       f"{value}{unit} by {pred_time}.")
            self.send_message(message, 'ops')
        
        return
        

    def send_message(self, message, channel):
        """
        Sends a message to the specified Telegram channel (ops or tech).
        
        Args:
            message (str): The message to be sent to Telegram.
            channel (str): The channel to which the message should be sent 
            ('ops' for operational, 'tech' for technical).
        
        Functionality:
            - Sends the message to either the operational or technical chat ID
              based on the channel parameter.
            - Uses the Telegram Bot API to send the message.
        """
        if channel == 'ops':
            chat_id = self.ops_chat_id
        else:
            chat_id = self.tech_chat_id

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message
        }
        response = requests.post(url, data=payload)
        if not response.ok:
            print(f"Failed to send message: {response.text}")
        

def test_client(BOT_TOKEN, OPS_CHAT_ID, TECH_CHAT_ID):

    bot = Telebot(BOT_TOKEN, OPS_CHAT_ID, TECH_CHAT_ID)

    # Example usage of the log function
    current_time = "20:29"
    bot.log(
        error_type=1,
        value=0.0,
        time=current_time,
        sensor='Temperature Sensor',
        unit='°C'
    )

    bot.log(
        error_type=2,
        value=105,
        time=current_time,
        sensor='pH Sensor',
        unit='pH',
        cl=100
    )

    bot.log(
        error_type=3,
        value=8.5,
        time=current_time,
        sensor='Dissolved Oxygen Sensor',
        unit='mg/L',
        pred_time='21:00',
        range_up=10.0,
        range_low=5.0
    )

    print('Messages sent successfully.')

if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    load_dotenv()
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    OPS_CHAT_ID = os.getenv("OPS_CHAT_ID")
    TECH_CHAT_ID = os.getenv("TECH_CHAT_ID")
    test_client(BOT_TOKEN, OPS_CHAT_ID, TECH_CHAT_ID)