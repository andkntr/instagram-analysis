from sqlalchemy import create_engine

# 接続文字列の作成
connection_string = 'mysql+mysqlconnector://b58325833839d0:f0fb916b@us-cdbr-east-06.cleardb.net/heroku_dd21f3af32a652e'

# エンジンの作成
engine = create_engine(connection_string)

# コネクションの確立
with engine.connect() as conn:
    # クエリの実行など、必要な処理を行う
    result = conn.execute('SELECT * FROM users')
    for row in result:
        print(row)

#engine = create_engine('mysql+mysqlconnector://b58325833839d0:f0fb916b@us-cdbr-east-06.cleardb.net/heroku_dd21f3af32a652e', echo=True)
