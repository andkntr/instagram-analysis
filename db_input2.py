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
import time
import pytz
from datetime import datetime
import sys

"""
'816my__',
'___5_48',
'____ash108',
'__mi_so_ko__',
'_aleka91',
'_ayakokawai_',
'_kn_616',
'_mayucosme',
'_misaa73',
'_skin.jp',
'_yukimiyazaki_',
'a_yann.msk.ntk',
'africa_onnanoko',
'ainarashi617',
'aisuchan_ss',
'akanuke_make_',
'akarin_beauty_channel',
'alice_kaiseki',
'ami_tanimoto.official',
'amitea67',
'ao1_beauty',
'aoi_blube',
'appleapple19xx',
'aristagram38',
'asaren77',
'asuka_imo',
'ave_biyou',
'aya_mym_a',
'ayami.0624',
'azu_k02',
'bamli.tokyo',
'bb_jp_nakamu',
'beauty_mmr',
'before_after89',
'bihada.ni.naru',
'bihada_care_channel',
'bihada_maniac',
'biyo_nurse',
'biyou.nurse.chicchi',
'biyouiryou_media',
'blube_cosme',
'bluebase_winter8',
'bluebe_matome',
'borotama9',
'brg_magazine',
'buruchan.1.7',
'cc_chihiro_yakugaku',
'chan____ara',
'chanmaru_cosme_',
'charico2019',
'cherie_latte_official',
'chicchimity',
'chiiko_22111',
'china___makeup',
'cjmrm_f',
'clear.skin_beauty',
'coco_home__',
'cocoasa01',
'cosmate_2021',
'cosme__roa',
'cosme_bunny_ch',
'cosme_room_',
'cosmelove.korea',
'cosmenprotein',
'cosmesalan',
'cotoha.bihada.labo',
'dayday_blog',
'dngnchan',
'eimy_official',
'ema_kamakulife',
'emirin_style',
'emme_cosme',
'enukosme_biyo',
'eye__pallet',
'fuchan_2003',
'h1t0mi_0210',
'haaaxxcosme',
'haasan_haru',
'hana.chocolate.taku',
'hana___skin',
'hani_korea.cosme',
'harue_beautycreate',
'harupon.15',
'hau_beauty_room',
'hii_chachacha',
'hima323232',
'hinano___days',
'hitomi1218',
'homcos_beauty_',
'honey_chiaki07',
'hp7nk_rika',
'hystericm8n',
'i.k.1986',
'ichrin_2020',
'iebe_cosme',
'iebe_lab',
'io_aoyama',
'iwbcd___6',
'k__n__k__n__',
'kaho.skincare',
'kamicosme_trend',
'kanan_cosme',
'kaorigram61',
'kaoyase_fight',
'karin__life',
'karinnn000',
'keana_method',
'kei_marunouchi.ol',
'kenbihada_second',
'kii.beauty_',
'kisumi0315',
'kmr_119',
'kobamo___cc',
'kohinata_moke',
'komekichiiii',
'korea_yura',
'kuma_chika888',
'kuucosme',
'kyuru_kyurumi',
'lana__cosme',
'leina810',
'lily_70626',
'love_miyuka',
'luvu_cosme',
'm_loves_kcosme',
'ma.yu8980',
'maho_713',
'maimero.iciigomilk.y',
'make_up_hitoe',
'makiiiy_makeup',
'manami__beauty',
'mari_fasting',
'maru959595959595',
'matakichi_38',
'mayday_kr',
'mayu_cosme_',
'mcendo1027_beauty',
'meg.948',
'meili_diary',
'mens___ichi',
'mens_bihada_yuma',
'mens_biyou_puchipura',
'mens_skincare_takkun',
'mensbeauty_cosme_d',
'menscosme_23',
'meru_cosme',
'mii___0228',
'mii_atlanta',
'miki_kurasi',
'miku_cosmake',
'mimiovomimi',
'minamininaritaiol',
'mint_beautylab',
'mirimanialove',
'misamisa_714',
'misora_cosmetics',
'mite_beauty',
'miyuki_beautylog',
'mizumoto_kahogram',
'mmk.87_',
'mo.mo.mama',
'moku_hico',
'momo___cosme',
'momochan_kakei',
'momomo1572',
'momoringo_5',
'moomie_official',
'mote_bunseki',
'mrmrmron7',
'mu_1922',
'mwnail.kt',
'myme_dlaw',
'na.na1557',
'nachaaaaki',
'nagi_30life_',
'nako__make',
'namu6331',
'nanamin_skincare',
'nao__nao__705',
'natsum10803',
'navi_cosbeauty',
'nenbi_cosme',
'nera.beauty',
'nishoku_a34',
'noako_cosme',
'nuusan_beauty',
'o_bury_',
'ol__milktea___',
'ossy_beautylog',
'otona.danshi_labo',
'otona_no_cosme',
'pickmi.beauty',
'pink___yuri',
'pinkcandy_8',
'pipi_lilly',
'pomemama_biyou',
'puchipuracosme_ranking',
'r21.cosme',
'ranran_ol',
'real_chan_cosme',
'rena_cosme_',
'rena_rena.biyou',
'rikoniko.cosme',
'rina072116',
'rinrin934',
'rise_n0910',
'roomuji_yume',
'ruluu112t',
'ruru_hitoe_okubutae',
'ryoko_makeup',
'sa_e._',
'sachi.nail_',
'saesaesaeko03',
'sakiiiii20',
'sakura__cosme',
'saori000917',
'sari_pink144',
'schk_maru',
'sekiyumiko0821',
'serababy_',
'shihosan_star',
'shiro_tuki1',
'shushu_223_',
'siratama.cosme',
'skincare.media.bihada',
'skincare_8_',
'skincare_kasumi',
'skincare_maniac_',
'skincare_prk',
'sklabo14130',
'smilesss_beauty',
'sou_mensbiyou',
'su_beauty_life',
'sugar_moon_lala',
'summer_zyoshi',
'tackey_mens_skincare',
'tantaline15',
'tarako___v',
'tenko0517',
'tiara7_momo',
'tomato4researcher',
'tomoko_cosme',
'tsukiko_kotsuki',
'tsuruas',
'umino_yuki',
'urico.sme',
'usayoshi_cosme',
'wannyan_29',
'welcome.baby1122',
'woman_from30',
'x_xx_mari_xx_x',
'xoxo_yuuuuu_xoxo',
'y_n_a79',
'yapoi114',
'yo_shi_no_n_',
'yoppy__0206',
'yu11_m',
'yui_cool.make',
'yukarii_32beauty',
'yuki_mens_beauty',
'yuko.love.beauty_yummy',
'yuri.littlebylittle',
'yurikocosme',
'yuu_cosmelife',
'yuu_mens_beauty',
'yuuri_ftb',
'yuusan0600',
'coslabo39',
'100nichi_cosme',
'4yu.mama',
'5353mik',
'__.04milcosme__',
'______ohyoume',
'___aisakura.co',
'__mefil',
'__pikosme',
'_arisa_212_',
'_kanon_jp',
'_koba_poco_',
'_mellowwaves',
'_piyogram_',
'_yadonchan',
'a_make_myhobby',
'achan_beautygram',
'agingcare_around30',
'airi_innercare',
'aju_245',
'akanukebijo',
'ako_p49',
'alohakkooo_39',
'amiice17',
'anin.beautyyy',
'aobase_cosme',
'aoi_style111',
'arielastc2',
'asabiyou_',
'asuka12_09',
'autumn_zyoshi',
'aya.bangkok',
'ayako__miyata',
'ayami_yamichan',
'ayaokutsu',
'aza_mote.gram',
'knst_mr',
'ayannu61',
'ayapan_beauty',
'2021_hur',
'azu_cosmeotaku',
'azuayu',
'beauty_mentalist_yuya',
'beautyurigram',
'berry.3b',
'bihada_katu',
'bitan__channel',
'biyou.bot.kotono',
'biyou_hifuko',
'biyoushin_413',
'blue_mayu_',
'bluebe_chan',
'bm_krh',
'bubblism0310',
'bytheharu',
'chachakun_cosme',
'chanmaai8888vlog',
'chansan_makeup',
'charmix2020',
'chi.cosme07',
'chieru_40biyo',
'chika24rgrm',
'chon_._._',
'ckumacom',
'clearocean_n',
'cocoa.x2',
"""







#https://graph.facebook.com/v11.0/17841409036181634?access_token=EAAZAcedTNjXsBALdBhB9weQw3OQ8tEhuUDQ8skdJqUiTdeU2ZAhojkvZCnZATJkdXhu70gZAelMZCWtCcI3WSReZCm8nICRMiglLtzymqEOECXPYWm0ITI8Fl799RWfmHJxu97qczwDKAGbdbzwZCT8gcWSiwaQbX7ZA8AAlDonSEowAznCvJxQWT

"""
makiiiy_makeup
maimai.007
kurumi_hanji
"""


reg_list = [
'hiiiiiiiic.77',
]


#user_id='rikoniko.cosme'
app_secret = '6128b0c096c90f89f69ad760b5104750'
app_id = '1790528191303035'
# アクセス情報
business_account_id = '17841409036181634'
#token = 'EAAZAcedTNjXsBAF5johzIDm9DL71JMpVb1zccX3tqhzwLoCNk0WRz6u4IIFRkKLzRkfRdI4eZBqFZAx71hPnaJF8Bx1rEUefSftfWWZBo5n2pnfXMKktQ3ZCvbqsllxjQSlxJ0HLdKK9LxCmJvwGHXY2lZCboIoL9QCVGACWZCugdjhfiVY4FEg63THr48vfDZCdNzZCdfFUHFQZDZD'
token='EAAZAcedTNjXsBALdBhB9weQw3OQ8tEhuUDQ8skdJqUiTdeU2ZAhojkvZCnZATJkdXhu70gZAelMZCWtCcI3WSReZCm8nICRMiglLtzymqEOECXPYWm0ITI8Fl799RWfmHJxu97qczwDKAGbdbzwZCT8gcWSiwaQbX7ZA8AAlDonSEowAznCvJxQWT'


fields = 'name,biography,follows_count,followers_count,media_count'
media_fields = 'like_count,comments_count'
engine = create_engine('mysql+mysqlconnector://b58325833839d0:f0fb916b@us-cdbr-east-06.cleardb.net/heroku_dd21f3af32a652e', echo=True, pool_size=10, max_overflow=20)

# リトライ回数とリトライ間隔の設定
MAX_RETRIES = 5
RETRY_INTERVAL = 5  # seconds


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




#user_info(business_account_id,token,user_id,fields)
#print(user_info_list)

"""
#データ可視化工程

result = user_media_info(business_account_id,token,user_id,media_fields)

df_concat=None
df_concat=pd.DataFrame(result[0])

if len != 1:
    for i,g in enumerate(result):
        df_concat = pd.concat([pd.DataFrame(result[i]),df_concat])
"""
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


"""
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
"""
except KeyError:
    print('error')
    user_info_list.append('NULL')
"""


    
def insert_mysql():
    engine = create_engine('mysql+mysqlconnector://b58325833839d0:f0fb916b@us-cdbr-east-06.cleardb.net/heroku_dd21f3af32a652e', echo=True, pool_size=10, max_overflow=20)
    df = pd.DataFrame([user_info_list], columns=['id','name','follow','follower','media','bio','eng','created_at'])
    df.to_sql('users',con=engine, if_exists='append', index=False)
    print('Successfully added to db')




for t,user_id in enumerate(reg_list):
    # user_idが奇数の場合は処理をスキップする
    if t % 2 != 0:
        print(user_id + ': Skipping due to odd user_id.')
        continue
    try:
        user_info_list=[]
        print(user_id+': User Info Obtaining')
        user_info(business_account_id,token,user_id,fields)
        result = user_media_info(business_account_id,token,user_id,media_fields)
        
        print(user_info)
        print(result)
        df_concat=None
        df_concat=pd.DataFrame(result[0])

        if len != 1:
            for i,g in enumerate(result):
                df_concat = pd.concat([pd.DataFrame(result[i]),df_concat])
        iine = df_concat['like_count'].sum() #いいね数の合計
        print('いいね数'+str(iine))
        komento = df_concat['comments_count'].sum() #コメント数の合計
        post = len(df_concat)
        follower = user_info_list[3]
        follower = int(follower)
        eng_rate = ((iine+komento)/post)/follower*100
        eng_rate = round(eng_rate,2)
        user_info_list.append(eng_rate)
        # JSTタイムゾーンを設定
        jst_tz = pytz.timezone('Asia/Tokyo')
        created_at = datetime.now(tz=jst_tz).strftime("%Y-%m-%d")
        user_info_list.append(created_at)
        with engine.connect() as con:
            sql_check = text("SELECT id FROM users WHERE id = :user_id")
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
            conn.ping(reconnect=True)

            try:
                # DELETE文を実行
                sql = "DELETE FROM users WHERE id = %s"
                value = (user_id,)
                cursor.execute(sql, value)

                # 変更を確定
                conn.commit()

                print("Delete successfully")
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
            time.sleep(1120)
        else:
            insert_mysql()
            print('User successfully added to the db')
    except Exception as e:
        pass
        print(e)
        print(user_id+' passed')
        time.sleep(1120)