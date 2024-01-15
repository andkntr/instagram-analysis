import mysql.connector

def delete_from_table():
    # MySQLデータベースへの接続
    conn = mysql.connector.connect(
        host='us-cdbr-east-06.cleardb.net',
        user='b58325833839d0',
        password='f0fb916b',
        database='heroku_dd21f3af32a652e'
    )
 
    cursor = conn.cursor()

    try:
        # DELETE文を実行
        sql = "DELETE FROM users WHERE id = %s"
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

delete_from_table()