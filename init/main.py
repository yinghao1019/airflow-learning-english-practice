import psycopg2
import requests
import json
YOUTUBE_ROOT = 'https://www.googleapis.com/youtube/v3'


def get_api():
    with open('./credentials/youtube_api.json', 'r') as fp:
        token = json.load(fp)['token']
        return token


def get_video_ids(playListId: str, api_key: str):
    url = YOUTUBE_ROOT + \
        f'/playlistItems?playlistId={playListId}&part=contentDetails&key={api_key}&maxResults=20'
    response = requests.get(url)

    if (response.status_code == 200):
        video_items = response.json().get("items")
        video_items = [item["contentDetails"].get(
            "videoId") for item in video_items]
    else:
        raise ConnectionRefusedError(f"can not success get API:{url} data!")
    return video_items


def get_video_item(video_id, api_key):
    video_id = ",".join(video_id)
    url = YOUTUBE_ROOT + \
        f'/videos?id={video_id}&part=id,snippet,statistics&key={api_key}'
    response = requests.get(url)
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
    else:
        raise ConnectionRefusedError(f"can not success get API:{url} data! \n status code:{response.status_code}")
    return video_items


def get_channel_info(video: str, api_key: str):
    url_video = YOUTUBE_ROOT+f'/videos?id={video}&key={api_key}&part=snippet'
    response_video = requests.get(url_video).json()
    video_snippet = (response_video['items'][0]).get('snippet')
    channelId = video_snippet .get('channelId')
    channelTitle = video_snippet.get('channelTitle')
    print(channelId, ' ', channelTitle)

    url_channel = YOUTUBE_ROOT + \
        f'/channels?part=contentDetails&id={channelId}&key={api_key}'

    response_channel = requests.get(url_channel)
    if (response_channel.status_code == 200):
        response_channel = response_channel.json()
        playListId = response_channel['items'][0].get('contentDetails').get('relatedPlaylists').get('uploads')
    else:
        raise ConnectionRefusedError(f"can not success get API:{url_channel} data!")    
    return channelId, channelTitle, playListId


def insert_channel(channelId, playListId, channelTitle):
    connection=None
    try:
        connection = psycopg2.connect(
            database="airflow",
            user="airflow",
            password="airflow",
            host="database",
            port='5432'
        )
        cursor = connection.cursor()
        query = f'''INSERT INTO "channel"(channel_id, playlist_id, title) VALUES(%s, %s, %s);'''
        cursor.execute(query, (channelId, playListId, channelTitle))
        connection.commit()
        print(f"{channelTitle} insert complete!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def insert_video(data):
    connection=None
    try:
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
        cursor.executemany(query, data)
        connection.commit()
        print(f"{channelTitle} video insert complete!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)
        
    finally:
        if (connection):
            cursor.close()
            connection.close()


if __name__ == '__main__':
    # 可以塞你想要的頻道
    data = ['jONQrBkc8Wc','AOIzIh6-q9A&t=547s', 'XV2H9CuQUm8', 'uLnmTXxpK0Q','M82n01-pyIs']
    token = get_api()
    for video in data:
        # 發送請求取得資料
        channelId, channelTitle, playListId = get_channel_info(video, token)
        print(channelId)
        print(channelTitle)
        print(playListId)
        video_ids = get_video_ids(playListId, token)
        video_items = get_video_item(video_ids, token)
        # 新增至DB
        insert_channel(channelId, playListId, channelTitle)
        insert_video(video_items)
