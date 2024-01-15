import sqlite3
import tkinter as tk
import tkinter.messagebox as tkm
import requests
import pandas as pd
import json
import mysql.connector
import glob
import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy.exc import SQLAlchemyError
import pytz
from datetime import datetime


user_id='mii_labo'
# アクセス情報
business_account_id = '17841409053822318'
token = 'EAAO764HG2gMBAFbv2jjHZC4yCUetgy7vXOALGiGEELZAE0TWtqBkn2bQKzcfPIx23SLUGupXVs3HYfP8Am2eIe3HVVRXG7RUDTzZC2RRws9bcGIDQQXiedkjtXblXVb0Y97j6yxWqU2JuZC4d1ShrzW5fXD1Dv3b2cbm6rStxDuVPaoeDi63'

fields = 'name,biography,follows_count,followers_count,media_count'
media_fields = 'like_count,comments_count'

#ユーザー情報をリスト化
#どこかでユーザーIDを
user_info_list = []
def user_info(business_account_id,token,username,fields):
    request_url = "https://graph.facebook.com/v11.0/{business_account_id}?fields=business_discovery.username({username}){{{fields}}}&access_token={token}".format(business_account_id=business_account_id,username=username,fields=fields,token=token)
    response = requests.get(request_url)
    res = response.json()['business_discovery']
    user_info_list.append(user_id)
    user_info_list.append(res['name'])
    user_info_list.append(res['follows_count'])
    user_info_list.append(res['followers_count'])
    user_info_list.append(res['media_count'])
    user_info_list.append(res['biography'])



    return user_info_list

def user_media_info(business_account_id,token,username,media_fields):
    all_response =[]
    request_url = "https://graph.facebook.com/v14.0/{business_account_id}?fields=business_discovery.username({username}){{media{{{media_fields}}}}}&access_token={token}".format(business_account_id=business_account_id,username=username,media_fields=media_fields,token=token)
    response = requests.get(request_url)
    result = response.json()['business_discovery']

    all_response.append(result['media']['data'])
    print(all_response)

    #過去分がある場合は過去分全て取得（１度に取得できるのは25件）

    if 'after' in result['media']['paging']['cursors'].keys():
        next_token = result['media']['paging']['cursors']['after']
        while next_token is not None:
            request_url = "https://graph.facebook.com/v14.0/{business_account_id}?fields=business_discovery.username({username}){{media.after({next_token}){{{media_fields}}}}}&access_token={token}".format(business_account_id=business_account_id,username=username,media_fields=media_fields,token=token,next_token=next_token)
            response = requests.get(request_url)
            result=response.json()['business_discovery']
            all_response.append(result['media']['data'])
            if 'after' in result['media']['paging']['cursors'].keys():
                next_token=result['media']['paging']['cursors']['after']
            else:
                next_token = None
    return all_response




user_info(business_account_id,token,user_id,fields)
print(user_info_list)


#データ可視化工程

result = user_media_info(business_account_id,token,user_id,media_fields)

df_concat=None
df_concat=pd.DataFrame(result[0])

if len != 1:
    for i,g in enumerate(result):
        df_concat = pd.concat([pd.DataFrame(result[i]),df_concat])
#df_concat_sort = df_concat.sort_values('timestamp').drop_duplicates('id').reset_index(drop='true')



#df_concat_sort.set_index('timestamp')
#df_concat_sort['timestamp'] = pd.to_datetime(df_concat_sort['timestamp'],format='%Y/%m/%d')
#df_concat_sort['年月'] = df_concat_sort['timestamp'].dt.strftime('%Y%m')
#df_concat_sort['年'] = df_concat_sort['timestamp'].dt.strftime('%Y')
#df_concat_sort['timestamp'] = df_concat_sort['timestamp'].astype(str)
#df_concat_sort[['date','time']]=df_concat_sort['timestamp'].str.split(' ',expand=True)
#df_concat_sort=df_concat_sort.drop('timestamp',axis=1)
#df_concat_sort=df_concat_sort.drop('id',axis=1)
#df_concat_sort = df_concat_sort.reindex(columns=['date','time','like_count','comments_count','caption','年月','年'])



#エンゲージメント率：(（いいね数＋コメント数）/ポスト)/フォロワー数
iine = df_concat['like_count'].sum() #いいね数の合計
print('いいね数'+str(iine))
komento = df_concat['comments_count'].sum() #コメント数の合計
post = len(df_concat)
follower = user_info_list[3]
follower = int(follower)
eng_rate = ((iine+komento)/post)/follower*100
eng_rate = round(eng_rate,2)
user_info_list.append(eng_rate)
"""
except KeyError:
    print('error')
    user_info_list.append('NULL')
"""

def create_db():
    db = 'ig_list.db'
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    sql = '''
    CREATE TABLE IF NOT EXISTS users(
    id TEXT PRIMARY KEY,
    name TEXT,
    follow INTEGER,
    follower INTEGER,
    media INTEGER,
    bio TEXT,
    eng REAL)'''

    cur.execute(sql)
    conn.commit()
    conn.close()

#DBにデータ追加
def insert():
    db = 'ig_list.db'
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    sql = '''
    INSERT into users (id,name,follow,follower,media,bio,eng) values(?,?,?,?,?,?,?)
    '''
    params = user_info_list
    cur.execute(sql,params)
    conn.commit()
    conn.close()




def delete_mysql():
    engine = create_engine('mysql+mysqlconnector://b58325833839d0:f0fb916b@us-cdbr-east-06.cleardb.net/heroku_dd21f3af32a652e', echo=True)
    try:
        with engine.begin() as con:
            sql = text("DELETE FROM users WHERE id = :user_id")
            params = {"user_id": user_id}
            con.execute(sql, params)
        print("Delete successful")
    except SQLAlchemyError as e:
        print("Error occurred:", str(e))


def insert_mysql():
    engine = create_engine('mysql+mysqlconnector://b58325833839d0:f0fb916b@us-cdbr-east-06.cleardb.net/heroku_dd21f3af32a652e', echo=True)
    #df = pd.DataFrame([user_info_list])
    #print(df)
    with engine.connect() as con:
        sql_check = text("SELECT id FROM users WHERE id = :user_id")
        params = {"user_id": user_id}
        result = con.execute(sql_check, params)
        existing_id = result.fetchone()

    if existing_id:
        df = pd.DataFrame([user_info_list], columns=['id','name','follow','follower','media','bio','eng'])
        df.to_sql('users',con=engine, if_exists='append', index=False)
        print('Successfully updated to db')
    else:
        df = pd.DataFrame([user_info_list], columns=['id','name','follow','follower','media','bio','eng'])
        df.to_sql('users',con=engine, if_exists='append', index=False)
        print('Successfully added to db')
   
 # JSTタイムゾーンを設定
jst_tz = pytz.timezone('Asia/Tokyo')
created_at = datetime.now(tz=jst_tz).strftime("%Y-%m-%d")
user_info_list.append(created_at)
# delete_mysql()関数の呼び出し
try:
    delete_mysql()
except Exception as e:
    print("Error occurred:", str(e))