import pandas as pd
import re
import collections
from sqlalchemy import create_engine
import mysql.connector
import glob

engine = create_engine('mysql+pymysql://root:@localhost:3306/iglist', echo=True)


sql = f"SELECT * FROM post_detail WHERE id = %s"
params = ('yuu_cosmelife2',)
with engine.connect() as con:
    rs = con.execute(sql, params)
    graph_data = rs.fetchall()
    df = pd.DataFrame(graph_data, columns=rs.keys())

df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
df['yobi']=df["date"].dt.strftime("%a")


# 曜日ごとに"like_count"を集計する
iine_by_yobi = df.groupby("yobi", as_index=False)["like_count"].sum()
comments_by_yobi = df.groupby("yobi", as_index=False)["comments_count"].sum()

print(iine_by_yobi)
cat_yobi = pd.Categorical(iine_by_yobi['yobi'], categories=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'], ordered=True)
df_sorted = iine_by_yobi.loc[cat_yobi.argsort()].reset_index(drop=True)
print(df_sorted)

# 結果を1つのデータフレームに結合する
result_df = pd.concat([iine_by_yobi["like_count"], comments_by_yobi["comments_count"]], axis=1)
result_df.columns = ['iine_by_yobi', 'comments_by_yobi']
result_df['total_by_yobi'] = result_df['iine_by_yobi'] + result_df['comments_by_yobi']


print(result_df)


"""
いいね、コメントグラフ用
"""
#年月ごとにいいねとコメントを集計

#年月をリスト化して、X値として設定
x_values = df['yobi'].to_list()
#いいねとコメント数をリスト化して、Y地に設定
y_value = result_df['iine_by_yobi'].to_list()
y2_value = result_df['comments_by_yobi'].to_list()
#data2 = cur2.fetchall()




"""
with engine.connect() as con:
    kwd = '韓国コスメ'

    sql = f"SELECT * FROM post_detail WHERE caption LIKE %s"
    params = (f"%{kwd}%",)
    rs = con.execute(sql, params)
    data = rs.fetchall()
    user_list = [i[0] for i in data]
    user_list=set(user_list)

    eng_rate = []
    for user_id in user_list:
        sql = f"SELECT follower FROM users WHERE id = %s"
        params = (user_id,)
        with engine.connect() as con:
            rs = con.execute(sql, params)
            data = rs.fetchall()
            eng_rate.append(data[0][0])



with engine.connect() as con:
    sql = f"SELECT * FROM post_detail LEFT JOIN users ON post_detail.id=users.id"
    params = (f"%{kwd}%",)
    rs = con.execute(sql)
    data = rs.fetchall()
    print(data)
    df = pd.DataFrame(data, columns=rs.keys())
    print(df)
    post_eng = (df['like_count']+df['comments_count'])/df['follower']
    print(post_eng)



with engine.connect() as con:
    rs = con.execute(f"SELECT id, hashtag1, hashtag2, hashtag3, hashtag4, hashtag5, hashtag6 FROM hashtag")
    hashtag = rs.fetchall()

print(hashtag)

engine = create_engine('mysql+pymysql://root:February03!@localhost:3306/iglist', echo=True)
user_id = '__pikosme'
sql = f"SELECT year_month_col, like_count, comments_count FROM post_detail WHERE id = %s"
params = (user_id,)
with engine.connect() as con:
    rs = con.execute(sql, params)
    data = rs.fetchall()
    df = pd.DataFrame(data, columns=rs.keys())
    print(df)
    print(df.columns)

"""
