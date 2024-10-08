import requests

class Telebot:
    def __init__(self, token, ops_chat_id, tech_chat_id):
        self.token = token
        self.ops_chat_id = ops_chat_id
        self.tech_chat_id = tech_chat_id

    def log(self, error_type, value, time, sensor, unit, cl=None, pred_time=None,
                  range_up=None, range_low=None):
        message = None

        if error_type == 1:
            # Stuck at zero
            if value in (0, 0.0, 0.00, 0.000):
                message = (f"Detected 'Stuck at Zero' error for sensor {sensor} at"
                           f" time {time}.")
            else:
                # Constant Value Error
                message = (f"Detected 'Constant Value Error' for sensor {sensor} at "
                           f"value {value}{unit} @ {time}.")
            self.send_message(message, 'tech')

        elif error_type == 2:
            # Cusum error
            message = (f"Sensor {sensor} has drifted or the precision has degraded. "
                       f"It is above the Control Limit ({cl}{unit}) with value "
                       f"{value}{unit} at time {time}.")
            self.send_message(message, 'tech')

        elif error_type == 3:
            # Prediction error
            message = (f"Sensor {sensor} is predicted to be outside of the optimal "
                       f"range ({range_low}{unit}, {range_up}{unit}) with the value "
                       f"{value}{unit} by {pred_time}.")
            self.send_message(message, 'ops')
        
        return
        

    def send_message(self, message, channel):
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