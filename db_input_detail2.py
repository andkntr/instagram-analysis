import sqlite3
import tkinter as tk
import tkinter.messagebox as tkm
import requests
import pandas as pd
import json
from sqlalchemy import create_engine
import mysql.connector
import glob
import time
import pymysql


reg_list = [
'mii_labo',
]

#karenokajima0318 ビジネスアカウントじゃないかも
#risa_beauty_830
#minchan_cosme

#nina__sisters
#ayako__miyata


#

#haana0424
#mcosmem
#nina__sisters
#kosuota_yuki
#_mayucosme
#tsukiko_kotsuki
#kanan_cosme
#piii_xx_01
#seira_brube
#make_mitsuki
#minchan_cosme
#sakuran.beauty
# アクセス情報
business_account_id = '17841409036181634'
token = 'EAAZAcedTNjXsBALdBhB9weQw3OQ8tEhuUDQ8skdJqUiTdeU2ZAhojkvZCnZATJkdXhu70gZAelMZCWtCcI3WSReZCm8nICRMiglLtzymqEOECXPYWm0ITI8Fl799RWfmHJxu97qczwDKAGbdbzwZCT8gcWSiwaQbX7ZA8AAlDonSEowAznCvJxQWT'

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


#index=['表示名','ユーザID','プロフィール','フォロー数','フォロワー数','投稿数','n']
#col=['name','username','biography','follows_count','followers_count','media_count','id']



#タイトル


#user_info(business_account_id,token,user_id,fields)


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
#print(df_concat_sort)


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

def create_db():
    engine = create_engine('mysql+pymysql://root:February03!@localhost:3306/iglist', echo=True)
        # Baseクラスを作成する
    Base = declarative_base()

    # post_detailテーブルのモデルクラスを作成する
    class PostDetail(Base):
        __tablename__ = 'post_detail2'

        id = Column(String(255), primary_key=True)
        like_count = Column(Integer)
        comments_count = Column(Integer)
        caption = Column(Text)
        permalink = Column(Text)
        year_month = Column(Text)
        year = Column(Text)
        date = Column(Text)
        time = Column(Text)
        media_type = Column(String(30))

    # post_detailテーブルを作成する
    Base.metadata.create_all(engine)



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
    user='root'
    password = 'February03!'
    host = 'localhost'
    port = 3306
    database = 'iglist'


    engine = create_engine('mysql+pymysql://root:February03!@localhost:3306/iglist', echo=True)
    df = pd.DataFrame(df_concat_sort)
    print(df.columns)
    df.to_sql('post_detail',engine, if_exists='append', index=False)



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
        insert_mysql()
        print(user_id+' successfully finished')
        time.sleep(1000)
    except Exception as e:
        print(user_id+' passed')
        print(e)
        time.sleep(1000)

#create_db()
