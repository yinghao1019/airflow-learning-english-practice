from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pytz
from airflow import DAG
from string import Template
from pathlib import Path
import psycopg2
import configparser
import smtplib
import os
# 讀取設定
default_args = {
    'owner': 'Howard Hung',
    'start_date': datetime(2022, 10, 24, 0, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
config = configparser.ConfigParser()
config.read(os.path.join(AIRFLOW_HOME, "application.ini"))
MAIL_CONFIG = config["mail"]
POSTGRES_CONN_ID = config["database"]["postgres_connection_id"]


def get_mail_message(video_info):
    # 取得影片,頻道資訊
    video_url = f"https://www.youtube.com/watch?v={video_info['video_id']}"
    body_detail = {"videoUrl": video_url, "channelTitle": video_info["channel_title"],
                   "title": video_info["title"], "desc": video_info["video_description"],
                   "viewCount": video_info["view_count"], "likeCount": video_info["like_count"],
                   "publish": video_info["published_time"]}
    # 建立信件
    message = MIMEMultipart()
    message["subject"] = f"【Airflow system】{datetime.now(pytz.timezone('Asia/Taipei')).date()} 英文vlog學習推薦"
    message["from"] = MAIL_CONFIG["sender"]
    message["to"] = MAIL_CONFIG["reciever"]
    path = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), "template/video_email.html")
    print(path)
    template = Template(Path(path).read_text())
    body = template.substitute(body_detail)
    message.attach(MIMEText(body, "html"))
    return message


def get_db_result(query, parameter):
    hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(query, parameter)
    result = cursor.fetchall()
    cursor.close()
    return result


def get_video_id():
    query = '''SELECT video.video_id FROM video WHERE video_id NOT IN (SELECT video_id FROM history)ORDER BY RANDOM() LIMIT 1;'''
    rows = get_db_result(query, None)
    if len(rows) == 1:
        return (rows[0])['video_id']
    else:
        return None


def have_video_or_not(ti):
    flag = ti.xcom_pull(task_ids='get_video_id', key='return_value')
    if flag == None:
        return 'do_nothing'
    else:
        return 'send_video'


def send_video(ti):
    video_id = ti.xcom_pull(task_ids='get_video_id', key='return_value')
    query = '''SELECT channel.title as channel_title,video.* FROM video LEFT JOIN channel on video.channel_id=channel.channel_id WHERE video.video_id=%(id)s'''
    result = get_db_result(query, {'id': video_id})[0]
    mail = get_mail_message(result)
    with smtplib.SMTP(host=MAIL_CONFIG["smtp_host"], port=MAIL_CONFIG["smtp_port"]) as smtp:
        try:
            smtp.ehlo()  # 驗證SMTP伺服器
            smtp.starttls()  # 建立加密傳輸
            smtp.login(MAIL_CONFIG["account"],
                       MAIL_CONFIG["password"])  # 登入寄件者gmail
            # 寄信
            smtp.send_message(mail)
        except Exception as error:
            raise ConnectionError(error)


with DAG(
    dag_id='generate_video',
    default_args=default_args,
    description='daily generate the video',
    schedule_interval='@daily'
) as dag:
    get_video_id_task = PythonOperator(
        task_id='get_video_id',
        python_callable=get_video_id
    )

    have_video_or_not_task = BranchPythonOperator(
        task_id='have_video_or_not',
        python_callable=have_video_or_not
    )

    send_video_task = PythonOperator(
        task_id="send_video",
        python_callable=send_video
    )
    insert_history_task = PostgresOperator(
        task_id='insert_history',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='''
                INSERT INTO history(dt, video_id) VALUES('{{ ds  }}', '{{ ti.xcom_pull(task_ids='get_video_id', key='return_value') }}');
        '''
    )

    do_nothing_task = EmptyOperator(task_id='do_nothing')

    get_video_id_task >> have_video_or_not_task >> [
        do_nothing_task, send_video_task]
    send_video_task >> insert_history_task
