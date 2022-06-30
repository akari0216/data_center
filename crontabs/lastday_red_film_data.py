import pandas as pd
import numpy as np
import pymysql
import ftplib
import re,os
import datetime
from sqlalchemy import create_engine
from logger import get_logger

today = datetime.date.today()
tyear = today.year
tmonth = today.month
tday = today.day
yesterday = str(today - datetime.timedelta(days = 1))

#从数据库获取数据
def read_sql(sql):
    conn = pymysql.connect(host = "192.168.16.114",port = 3306,user = "root", passwd = "jy123456",db = "film_data",charset = "utf8")
    res = pd.read_sql(sql,conn)
    df = pd.DataFrame(res)
    return df

#写入数据
def to_sql(df,tablename):
    conn = create_engine("mysql+pymysql://root:jy123456@192.168.16.114:3306/film_data?charset=utf8")
    df.to_sql(tablename,con = conn,if_exists = "append",index = False)

#获取ftp数据
def get_csv_data(date):
    df = pd.DataFrame()
    ftp = ftplib.FTP()
    ftp.connect(host = "172.20.240.195",port = 21 ,timeout = 30)
    ftp.login(user = "sjzx",passwd = "jy123456@")
    ftp.set_pasv(False)
    datestr = date.replace("-","")
    filename = "SessionRevenue_" + datestr + ".csv"
    ftplist = ftp.nlst()
    for each_file in ftplist:
        judge = re.match(filename,each_file)
        if judge:
            file_handle = open(filename,"wb+")
            ftp.retrbinary("RETR " + filename,file_handle.write)
            #务必要添加关闭语句，否则只有程序结束了文件才被解锁
            file_handle.close()           
            df = pd.read_csv(filename,encoding = "utf-8")
            os.remove(filename)

            
    ftp.quit()
    return df

def red_film_list():
    sql_film_list = "select red_film from red_film_list"
    film_list = read_sql(sql_film_list)["red_film"].tolist()
    return film_list

def cinema_area():
    sql_cinema_area = "select vista_cinema_name,cinema_name,city,film_center from jycinema_info"
    df_cinema_area = read_sql(sql_cinema_area)
    return df_cinema_area

def red_film_run(df,date):
    df_table_red_film = pd.DataFrame()
    pat = "（.*?）\s*|\(.*?\)\s*|\s*"
    df["影片"].replace(pat,"",regex = True,inplace = True)
    df = df[df["场次状态"].isin(["开启","已计划","已批准"])]
    status_list = ["已批准","已计划","开启"]
    red_film_lst = red_film_list()
    df = df[df["影片"].isin(red_film_lst)]
    df["日期"] = df["场次时间"].replace(" \d\d:\d\d:\d\d","",regex = True)
    df_cinema_area = cinema_area()
    #红片时间筛选
    df["红片时间1"] = df["场次时间"].replace("\d\d:\d\d:\d\d","07:00:00",regex = True)
    df["红片时间1"] = pd.to_datetime(df["红片时间1"],format = "%Y-%m-%d %H:%M:%S")
    df["红片时间2"] = df["场次时间"].replace("\d\d:\d\d:\d\d","23:00:00",regex = True)
    df["红片时间2"] = pd.to_datetime(df["红片时间2"],format = "%Y-%m-%d %H:%M:%S")
    df["场次时间"] = pd.to_datetime(df["场次时间"],format = "%Y-%m-%d %H:%M:%S")
    df["判断1"] = df["场次时间"] >= df["红片时间1"]
    df["判断2"] = df["场次时间"] < df["红片时间2"]
    df["判断1"] = df["判断1"].apply(lambda x:1 if x is True else 0)
    df["判断2"] = df["判断2"].apply(lambda x:1 if x is True else 0)
    df_red_film = df[df["判断1"].isin([1]) & df["判断2"].isin([1])]
    df_red_film_abnormal = df[df["判断1"].isin([0]) | df["判断2"].isin([0])]
    if len(df_red_film) != 0:
        df_red_film = df_red_film[df_red_film["日期"].astype(str).isin([date])]
        df_red_film["场次值"] = 1
        df_table_red_film = pd.pivot_table(data= df_red_film, index = ["影院"], values = ["场次值"], aggfunc = [len], fill_value = 0, margins = False)
        df_table_red_film.reset_index(inplace = True)
        df_table_red_film2 = df_table_red_film[["影院"]]
        table_columns = df_table_red_film.columns.level[2]
        for each_status in status_list:
            if each_status in table_columns:
                df_table_red_film2[each_status] = df_table_red_film["len"]["场次值"][each_status]
            else:
                df_table_red_film2[each_status] = 0
        df_table_red_film2["总计"] = df_table_red_film2["开启"] + df_table_red_film2["已批准"] + df_table_red_film2["已计划"]
        df_table_red_film2.columns = df_table_red_film2.columns.get_level_values(0)
        df_table_red_film2.reset_index(inplace = True)
        # df_table_red_film = pd.pivot_table(data = df_red_film,index = ["影院"],values = ["影片"],aggfunc = {"影片":len},fill_value = 0,margins = False)
        # df_table_red_film.reset_index(inplace = True)
        df_table_red_film = pd.merge(left = df_table_red_film,right = df_cinema_area,how = "left",left_on = "影院",right_on = "vista_cinema_name")
        df_table_red_film.drop(columns = ["影院","vista_cinema_name"],axis = 1,inplace = True)
        df_table_red_film.rename(columns = {"cinema_name":"cinema","已批准":"status_approve","已计划":"status_plan","开启":"status_open","总计":"status_total"},inplace = True)
        df_table_red_film["op_date"] = date
        df_table_red_film["fetch_date"] = today
        df_table_red_film = df_table_red_film.reindex(columns = ["cinema","city","film_center","status_open","status_plan","status_approve","status_total","op_date","fetch_date"])

    if len(df_red_film_abnormal) != 0:
        df_red_film_abnormal = df_red_film_abnormal[["影院","影厅","影片","场次时间","票房","人数","场次状态","总座位数","上座率"]]
        df_red_film_abnormal["场次时间"]= df_red_film_abnormal["场次时间"].replace("\d\d\d\d-\d\d-\d\d ","",regex = True,inplace = False)
        df_red_film_abnormal = pd.merge(left = df_red_film_abnormal,right = df_cinema_area,how = "left",left_on = "影院",right_on = "vista_cinema_name")
        df_red_film_abnormal.drop(columns = ["影院","vista_cinema_name"],axis = 1,inplace = True)
        df_red_film_abnormal.rename(columns = {"cinema_name":"cinema","影厅":"hall","影片":"film","场次时间":"session_time","票房":"bo","人数":"people","总座位数":"seats","上座率":"occupancy","场次状态":"session_status"},inplace = True)
        df_red_film_abnormal["op_date"] = date
        df_red_film_abnormal["fetch_date"] = today

    return df_table_red_film,df_red_film_abnormal

set_logger = get_logger("/home/log/film_data","lastday_red_film_data")
table_list = ["red_film_data","red_film_abnormal"]
#先清除昨天之后的红片数据
datestr = [str(today - datetime.timedelta(days = 1))]
conn = pymysql.connect(host = "192.168.16.114",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
cursor = conn.cursor()
for each_table in table_list:
    cursor.execute("delete from %s where op_date >= '%s' and fetch_date in ('%s','%s')" % (each_table,yesterday,yesterday,str(today)))
    set_logger.info("delete table %s date %s completed" % (each_table,yesterday))

#加入昨天的数据
print("正在处理%s数据" % yesterday)
df = get_csv_data(yesterday)
df_red_film,df_red_film_abnormal = red_film_run(df,yesterday)
if len(df_red_film) != 0:
    to_sql(df_red_film,"red_film_data")
    set_logger.info("red_film_data completed %s" % yesterday)
if len(df_red_film_abnormal) != 0:
    to_sql(df_red_film_abnormal,"red_film_abnormal")
    set_logger.info("red_film_abnormal completed %s" % yesterday)