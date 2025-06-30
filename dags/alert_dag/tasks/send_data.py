from airflow.decorators import task

from config.data_sources.telegram_api import Telegram

@task
def send_data(data):
    tg = Telegram()
    for d in data:
        tg.send_msg(d)