from airflow.decorators import task

from config.data_sources.telegram_api import Telegram

@task
def send_data(data):
    tg = Telegram()
    tg.send_message(data)