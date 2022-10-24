from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow import DAG
import requests
import json
import psycopg2
import configparser
import os
args = {
    'owner': 'Howard Hung',
    'start_date': datetime(2022, 10, 24, 10, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
config = configparser.ConfigParser()
config.read(os.path.join(AIRFLOW_HOME, "application.ini"))
YOUTUBE_BASE = config["youtube"]["api_base_url"]
MAX_REULTS = config["youtube"]["max_results"]
POSTGRES_CONN_ID = config["database"]["postgres_connection_id"]


def get_api_key():
    with open(os.path.join(AIRFLOW_HOME, 'dags/credentials/youtube_api.json'), 'r') as fp:
        token = json.load(fp)['token']
        return token


def get_channel_info():
    hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    query = '''SELECT channel_id, playlist_id FROM channel;'''
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    print(rows)
    return rows


def check_video_is_exit(video_id):
    hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    query = f'''SELECT video_id FROM video WHERE video_id = '{video_id}';'''
    cursor.execute(query)
    if cursor.fetchall():
        return True
    else:
        return False


def get_update_info(ti):
    data = ti.xcom_pull(task_ids='get_channel_info', key='return_value')
    video_do_not_store = []
    api_key = get_api_key()
    for info in data:
        channel_id, playlist_id = info
        url_playlist = f'{YOUTUBE_BASE}/playlistItems?part=contentDetails&playlistId={playlist_id}&key={api_key}&maxResults={MAX_REULTS}'
        response = requests.get(url_playlist)
        if (response.status_code == 200):
            response = response.json()
            for info in response['items']:
                video_id = info['contentDetails'].get('videoId')
            if check_video_is_exit(video_id):
                continue
            else:
                video_do_not_store.append(video_id)
        else:
            raise ConnectionError(
                f"can not success get API data! \n response:{response}")
    if len(video_do_not_store) == 0:
        return 'do_nothing'
    else:
        return video_do_not_store


def insert_db_or_not(ti):
    flag = ti.xcom_pull(task_ids='get_update_info_task', key='return_value')
    if flag == 'do_nothing':
        return 'do_nothing'
    else:
        return 'crawl_video'


def crawl_video(ti):
    datas = ti.xcom_pull(task_ids='get_update_info', key='return_value')
    api_key = get_api_key()
    for video_id in datas:
        response = requests.get(
            f'{YOUTUBE_BASE}/videos?id={video_id}&part=id,snippet,statistics&key={api_key}')
        if (response.status_code == 200):
            items = response.json().get("items")
            # 建立影片資料
            video_items = []
            for item in items:
                id = item.get('id')
                snippet = item.get('snippet')
                stat = item.get('statistics')
                video = {
                    'id': id,
                    'title': snippet.get('title'),
                    'desc': snippet.get('description')[:200],
                    'published': snippet.get('publishedAt'),
                    'channelId': snippet.get('channelId'),
                    'viewCount': stat.get('viewCount'),
                    'likeCount': stat.get('likeCount')
                }
                video_items.append(video)
            return video_items
        else:
            raise ConnectionError(
                f"can not success get API data! \n response:{response}")


def insert_db(ti):
    video_items = ti.xcom_pull(task_ids='crawl_video', key='return_value')
    connection = psycopg2.connect(
        database="airflow",
        user="airflow",
        password="airflow",
        host="database",
        port='5432'
    )
    cursor = connection.cursor()
    query = f'''INSERT INTO video(channel_id,video_id,title,video_description,published_time,view_count,like_count) 
        VALUES(%(channelId)s, %(id)s, %(title)s, %(desc)s,%(published)s,%(viewCount)s,%(likeCount)s);'''
    cursor.executemany(query, video_items)
    connection.commit()
    cursor.close()
    connection.close()


with DAG(
    dag_id='get_new_video',
    default_args=args,
    description='weekly get the video information',
    schedule_interval='@weekly'
) as dag:
    get_channel_info_task = PythonOperator(
        task_id='get_channel_info',
        python_callable=get_channel_info,
    )

    get_update_info_task = PythonOperator(
        task_id='get_update_info',
        python_callable=get_update_info,
    )

    insert_db_or_not_task = BranchPythonOperator(
        task_id='insert_db_or_not',
        python_callable=insert_db_or_not
    )

    crawl_video_task = PythonOperator(
        task_id='crawl_video',
        python_callable=crawl_video
    )

    insert_db_task = PythonOperator(
        task_id='insert_db',
        python_callable=insert_db
    )
    do_nothing_task = EmptyOperator(task_id='do_nothing')

    get_channel_info_task >> get_update_info_task >> insert_db_or_not_task
    insert_db_or_not_task >> crawl_video_task
    insert_db_or_not_task >> do_nothing_task
    crawl_video_task >> insert_db_task
