import os
import requests
import telegramify_markdown

from config.logger.logger import logger


class Telegram:
    """
    Telegram API client

    Attributes:
        url (str): Telegram Bot API URL
        chat_id (str): Chat ID
        thread_id (str): Thread ID
    """
    def __init__(self):
        """
        Initializes the Telegram client
        """
        self.url = os.environ.get('TG_URL')
        self.chat_id = os.environ.get('TG_CHAT_ID')
        self.thread_id = os.environ.get('TG_THREAD_ID')

    def send_msg(self, message: str) -> dict:
        """
        Sends a message to the configured topic of a certain Telegram chat

        Args:
            message (str): The message to send

        Returns:
            dict: The response from Telegram API
        """
        data = {
            'message_thread_id': self.thread_id,
            'chat_id': self.chat_id,
            'text': telegramify_markdown.markdownify(str(message)),
            'parse_mode': 'MarkdownV2'
        }
        print(data)
        print(f'{self.url}/sendMessage')
        response = requests.post(url=f'{self.url}/sendMessage', json=data)
        if response.status_code != 200:
            logger.error(f'Bad response from Telegram: {response.text}')
        else:
            logger.info('Messages successfully sent to Telegram')
        return response.json()