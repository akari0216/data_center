import requests
import time,datetime
import random
import hashlib
import pymysql
from sqlalchemy import create_engine
import os
import pandas as pd
import numpy as np
from logger import get_logger
import warnings

warnings.filterwarnings("ignore")

id = "jinyi"
code = "e3a5db997a012a55ec9055782d1a90d3"
timestamp = str(int(time.time()))
ran = random.randint(0,9223372036854775807)

hl = hashlib.md5()
pre_code = id + "_" + code + "_" + timestamp + "_" + str(ran)
hl.update(pre_code.encode(encoding = "utf-8"))
sig = hl.hexdigest()

def file_token():
    url = "http://db.topcdb.com/zapi/getapi?id=%s&timestamp=%s&signature=%s&code=%s" % (id,timestamp,sig,ran)
    r = requests.get(url = url)
    if r.json()["status"] == 1:
        token = r.json()["token"]
        time1 = str(int(time.time()))
        f = open("token.txt","w")
        file_str = str({"token":token,"time":time1})
        f.write(file_str)
        f.close()
        return token,time1

def get_token():
    token = ""
    timestr = ""
    filelist = os.listdir(".")
    if "token.txt" not in filelist:
        token,timestr = file_token()
    elif "token.txt" in filelist:
        f = open("token.txt","r")
        file_dict = eval(f.readlines()[0])
        f.close()
        s_token = file_dict["token"]
        s_time = file_dict["time"]
        now = int(time.time())
        #tokens' valid time is 300 seconds,exclude 10s for cache
        if (now - int(s_time)) < 300:
            token = s_token
            timestr = s_time
        else:
            token,timestr = file_token()
            
    return token,timestr

#数据库连接
def database(db):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = db,charset = "utf8")
    return conn

conn_film_data  = database("film_data")
conn_jycinema_data = database("jycinema_data")

#读取mysql,返回dataframe
def read_sql(sql,conn):
    if "delete" in sql:
        cursor = conn.cursor()
        cursor.execute(sql)
    else:
        res = pd.read_sql(sql,conn)
        df = pd.DataFrame(res)
        return df
    conn.close()

#写入数据
def to_sql(df,tablename):
    conn = create_engine("mysql+pymysql://root:jy123456@localhost/film_data?charset=utf8")
    df.to_sql(tablename,con = conn,if_exists = "append",index = False)

def get_cinema_info():
    jycinema_sql = "select cinema_code,cinema_name,city,film_center from jycinema_info where op_status = 1"
    df_jycinema_info = read_sql(jycinema_sql,conn_film_data)
    compete_cinema_sql = "select jycinema_code,jycinema_name,compete_cinema_code,compete_cinema_name from compete_cinema_info"
    df_compete_cinema_info = read_sql(compete_cinema_sql,conn_jycinema_data)
    #将合并影城的编码进行还原
    df_compete_cinema_info_combine = df_compete_cinema_info[df_compete_cinema_info["jycinema_code"].isin(["111","222","333"])]
    df_compete_cinema_info = df_compete_cinema_info[~df_compete_cinema_info["jycinema_code"].isin(["111","222","333"])]
    data_combine = {"cinema_code":["44111201","44116601","44004101","44002121","35021202","35022701"],\
        "cinema_name":["中山石岐店","中山石岐二店","广州百信店","广州百信西区IMAX店","厦门明发店","厦门明发IMAX店"],\
        "jycinema_code":["111","111","222","222","333","333"]}
    df_combine = pd.DataFrame(data = data_combine)
    df_combine = pd.merge(left = df_combine,right = df_compete_cinema_info_combine,on = "jycinema_code",how = "left")
    df_combine.drop(columns = ["jycinema_code","jycinema_name"],axis = 1,inplace = True)
    df_combine.rename(columns = {"cinema_code":"jycinema_code","cinema_name":"jycinema_name"},inplace = True)
    df_compete_cinema_info = pd.concat([df_compete_cinema_info,df_combine],ignore_index = True)

    df_compete_cinema_info["compete_relation"] = "竞对"
    df_compete_cinema_info = pd.merge(left = df_compete_cinema_info,right = df_jycinema_info[["cinema_code","city","film_center"]],left_on = "jycinema_code",right_on = "cinema_code",how = "left")
    df_compete_cinema_info.drop(columns = ["cinema_code"],axis = 1,inplace = True)
    df_jycinema_info["compete_cinema_code"]  = df_jycinema_info["cinema_code"]
    df_jycinema_info["compete_cinema_name"] = df_jycinema_info["cinema_name"]
    df_jycinema_info["compete_relation"] = "直营"
    df_jycinema_info.rename(columns = {"cinema_code":"jycinema_code","cinema_name":"jycinema_name"},inplace = True)
    total_df = pd.DataFrame(columns = df_compete_cinema_info.columns.tolist())
    for each_cinema_code in df_jycinema_info["jycinema_code"].tolist():
        each_df_jycinema_info = df_jycinema_info[df_jycinema_info["jycinema_code"].isin([each_cinema_code])]
        each_df_compete_cinema_info = df_compete_cinema_info[df_compete_cinema_info["jycinema_code"].isin([each_cinema_code])]
        total_df = pd.concat([total_df,each_df_jycinema_info,each_df_compete_cinema_info],ignore_index = True)
    
    total_df = total_df.reindex(columns = ["jycinema_code","jycinema_name","compete_cinema_code","compete_cinema_name","compete_relation","city","film_center"])
    return total_df

  
id = "jinyi"
today = datetime.date.today()
begin_date = str(today)
end_date = str(today + datetime.timedelta(days = 14))
token = get_token()[0]
api = "https://db.topcdb.com/zapi/getpreselldata?id=%s&date=%s&enddate=%s&token=%s"
r = requests.get(api % (id,begin_date,end_date,token))
if r.json()["status"] == 1:
    data = r.json()["data"]
    df = pd.DataFrame(data)
    total_page = r.json()["totalpage"]
    for i in range(1,total_page +1):
        r2 = requests.get((api + "&page=%s") % (id,begin_date,end_date,token,i))
        data2 = r2.json()["data"]
        df2 = pd.DataFrame(data2)
        df = pd.concat([df,df2],ignore_index = True)
         
    df_cinema_info = get_cinema_info()
    total_df = pd.merge(left = df_cinema_info,right = df,left_on = "compete_cinema_code",right_on = "cinema_code",how = "left")
    total_df["show_date"] = pd.to_datetime(total_df["showtime"],format = "%Y-%m-%d")
    total_df["show_time"] = total_df["showtime"].str.slice(11,19)
    total_df.drop(columns = ["cinema_code","cinema_name","movie_id","showtime"],axis = 1,inplace = True)
    total_df["fetch_date"] = today
    total_df = total_df.reindex(columns = ["jycinema_code","jycinema_name","compete_cinema_code","compete_cinema_name","compete_relation","city","film_center","movie_name",\
                                           "show_date","show_time","price","sold_count","hall_name","seats","version","fetch_date"])
    total_df.drop_duplicates(keep = "first",inplace = True)
    
    set_logger = get_logger("/home/log/film_data","maoyan_presale_price")
    #先清空数据，再写入
    del_sql = "delete from maoyan_presale_price"
    read_sql(del_sql,conn_film_data)
    to_sql(total_df,"maoyan_presale_price")
    set_logger.info("maoyan_presale_price update completed")
    print("update completed!")
    