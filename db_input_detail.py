import sqlite3
import tkinter as tk
import tkinter.messagebox as tkm
import requests
import pandas as pd
import json
from sqlalchemy import create_engine
import mysql.connector
import glob
from sqlalchemy import create_engine,text, Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
import time
import mysql.connector

reg_list = [
'hiiiiiiiic.77'
]

pd.set_option('display.max_columns', None)
user_id='borotama9'

# アクセス情報
business_account_id = '17841409053822318'
token = 'EAAO764HG2gMBAFbv2jjHZC4yCUetgy7vXOALGiGEELZAE0TWtqBkn2bQKzcfPIx23SLUGupXVs3HYfP8Am2eIe3HVVRXG7RUDTzZC2RRws9bcGIDQQXiedkjtXblXVb0Y97j6yxWqU2JuZC4d1ShrzW5fXD1Dv3b2cbm6rStxDuVPaoeDi63'

fields = 'name,username,biography,follows_count,followers_count,media_count'
media_fields = 'username,timestamp,like_count,comments_count,caption,permalink,media_type'
# ユーザー情報を取得する
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

user_info(business_account_id,token,user_id,fields)


def user_media_info(business_account_id,token,username,media_fields):
    all_response =[]
    request_url = "https://graph.facebook.com/v14.0/{business_account_id}?fields=business_discovery.username({username}){{media{{{media_fields}}}}}&access_token={token}".format(business_account_id=business_account_id,username=username,media_fields=media_fields,token=token)
    response = requests.get(request_url)
    result = response.json()['business_discovery']

    all_response.append(result['media']['data'])

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

"""
result = user_media_info(business_account_id,token,user_id,media_fields)

#データ可視化工程

df_concat=None
df_concat=pd.DataFrame(result[0])

if len != 1:
    for i,g in enumerate(result):
        df_concat = pd.concat([pd.DataFrame(result[i]),df_concat])

df_concat_sort = df_concat.sort_values('timestamp').drop_duplicates('id').reset_index(drop='true')


df_concat_sort.set_index('timestamp')
df_concat_sort['timestamp'] = pd.to_datetime(df_concat_sort['timestamp'],format='%Y-%m-%dT%H:%M:%S%z')
df_concat_sort['year_month'] = df_concat_sort['timestamp'].dt.strftime('%Y%m')
df_concat_sort['year'] = df_concat_sort['timestamp'].dt.strftime('%Y')
df_concat_sort['timestamp'] = df_concat_sort['timestamp'].astype(str)
df_concat_sort[['date','time']]=df_concat_sort['timestamp'].str.split(' ',expand=True)
df_concat_sort=df_concat_sort.drop('timestamp',axis=1)
df_concat_sort=df_concat_sort.drop('id',axis=1)
#df_concat_sort = df_concat_sort.reindex(columns=['userid','date','time','like_count','comments_count','caption','permalink','year_month','year'])
df_concat_sort = df_concat_sort.rename(columns={'username': 'id'})
print(df_concat_sort)

"""
"""
#エンゲージメント率：(（いいね数＋コメント数）/ポスト)/フォロワー数
iine = df_concat['like_count'].sum() #いいね数の合計

komento = df_concat['comments_count'].sum() #コメント数の合計
post = len(df_concat)
follower = user_info_list[3]
follower = int(follower)
eng_rate = ((iine+komento)/post)/follower*100
eng_rate = round(eng_rate,2)
user_info_list.append(eng_rate)


df_graph_day=df_concat_sort.groupby('date').sum()[['like_count','comments_count']]
df_graph_nengetu=df_concat_sort.groupby('year_month').sum()[['like_count','comments_count']]
df_graph_year=df_concat_sort.groupby('year').sum()[['like_count','comments_count']]

"""



#DBにデータ追加
def insert():
    table_name = 'post_detail'
    conn=sqlite3.connect('ig_list.db')
    print(df_concat_sort)
    df_concat_sort.to_sql('post_detail',conn,if_exists="append",index=False)
    conn.commit()
    conn.close()
    """
    db = 'ig_list.db'
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    sql = '''
    INSERT into post_detail (id,date,time,like_count,comments_count,caption,permalink,year-month,year) values(?,?,?,?,?,?,?,?,?)
    '''
    df_concat_sort.to_sql(post_detail,conn,if_exists='fail',index=True)
    #params = user_info_list
    cur.execute(sql,params)
    conn.commit()
    conn.close()
    """

def insert_mysql():
    engine = create_engine('mysql+mysqlconnector://b58325833839d0:f0fb916b@us-cdbr-east-06.cleardb.net/heroku_dd21f3af32a652e', echo=True)
    df = pd.DataFrame(df_concat_sort)
    print(df.columns)
    df_concat_sort.to_sql('post_detail',engine, if_exists='append', index=False)



#create_db()

for t,user_id in enumerate(reg_list):
    try:
        print(t)
        print(user_id)
        result = user_media_info(business_account_id,token,user_id,media_fields)

        #データ可視化工程

        df_concat=None
        df_concat=pd.DataFrame(result[0])

        if len != 1:
            for i,g in enumerate(result):
                df_concat = pd.concat([pd.DataFrame(result[i]),df_concat])

        df_concat_sort = df_concat.sort_values('timestamp').drop_duplicates('id').reset_index(drop='true')


        df_concat_sort.set_index('timestamp')
        #print(df_concat_sort)
        df_concat_sort['timestamp'] = pd.to_datetime(df_concat_sort['timestamp'],format='%Y-%m-%dT%H:%M:%S%z')
        df_concat_sort['year_month'] = df_concat_sort['timestamp'].dt.strftime('%Y%m')
        df_concat_sort['year'] = df_concat_sort['timestamp'].dt.strftime('%Y')
        df_concat_sort['timestamp'] = df_concat_sort['timestamp'].astype(str)
        df_concat_sort[['date','time']]=df_concat_sort['timestamp'].str.split(' ',expand=True)
        df_concat_sort=df_concat_sort.drop('timestamp',axis=1)
        df_concat_sort=df_concat_sort.drop('id',axis=1)
        #print(df_concat_sort)
        #df_concat_sort = df_concat_sort.reindex(columns=['userid','date','time','like_count','comments_count','caption','permalink','year_month','year'])
        df_concat_sort = df_concat_sort.rename(columns={'username': 'id'})
        engine = create_engine('mysql+mysqlconnector://b58325833839d0:f0fb916b@us-cdbr-east-06.cleardb.net/heroku_dd21f3af32a652e', echo=True)
        with engine.connect() as con:
            sql_check = text("SELECT id FROM post_detail WHERE id = :user_id")
            params = {"user_id": user_id}
            result = con.execute(sql_check, params)
            existing_id = result.fetchone()
        if existing_id:
            print('User ID already exists in the db')
            conn = mysql.connector.connect(
                host='us-cdbr-east-06.cleardb.net',
                user='b58325833839d0',
                password='f0fb916b',
                database='heroku_dd21f3af32a652e'
            )
        
            cursor = conn.cursor()

            try:
                # DELETE文を実行
                sql = "DELETE FROM post_detail WHERE id = %s"
                value = (user_id,)
                cursor.execute(sql, value)

                # 変更を確定
                conn.commit()

                print("Delete successful")
            except mysql.connector.Error as e:
                # エラー処理
                print(f"Error occurred: {str(e)}")
                conn.rollback()
            finally:
                # データベース接続をクローズ
                cursor.close()
                conn.close()
            insert_mysql()
            print(user_id+' successfully finished')
            time.sleep(1200)
        else:
            insert_mysql()
            print('User successfully added to the db')
            time.sleep(1200)
    except Exception as e:
        pass
        print(e)
        print(user_id+' passed')
        time.sleep(1200)



