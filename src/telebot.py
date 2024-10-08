import telegram

class Telebot:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id