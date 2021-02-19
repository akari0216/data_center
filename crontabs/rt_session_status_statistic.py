import pandas as pd
import numpy as np
import pymysql
import ftplib
import re,os
import datetime
from sqlalchemy import create_engine
from logger import get_logger

today = datetime.date.today()
#从数据库获取数据
def read_sql(sql):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root", passwd = "jy123456",db = "film_data",charset = "utf8")
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
    engine = create_engine("mysql+pymysql://root:jy123456@localhost/film_data?charset=utf8")
    conn = engine.connect()
    df.to_sql(tablename,con = conn,if_exists = "append",index = False)

#获取ftp数据
def get_csv_data(date):
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

def statistic_run(df,date):
    pat = "（.*?）\s*|\(.*?\)\s*|\s*"
    df["影片"].replace(pat,"",regex = True,inplace = True)
    df["专资日期"] = (pd.to_datetime(df["场次时间"],format = "%Y-%m-%d") - datetime.timedelta(hours = 6)).astype(str).str.slice(0,10)
    df2 = df[df["专资日期"].isin([date]) & df["场次状态"].isin(["开启","已计划","已批准"])]
    if len(df2) != 0:
        df2["场次值"] = 1
        df_table = pd.pivot_table(df2,index = ["影院","影片"],columns = ["场次状态"],values = ["场次值"],aggfunc = [len],fill_value = 0,margins = False)
        df_table = pd.DataFrame(df_table.reset_index())
        df_table2 = df_table[["影院","影片"]]
        status_list = ["已批准","已计划","开启"]
        table_columns = df_table.columns.levels[2]
        for each_status in status_list:
            if each_status in table_columns:
                df_table2[each_status] = df_table["len"]["场次值"][each_status]
            else:
                df_table2[each_status] = 0

        df_table2["总计"] = df_table2["已批准"] + df_table2["已计划"] + df_table2["开启"]
        df_table2.columns = df_table2.columns.get_level_values(0)
        df_table2.reset_index(drop = True,inplace = True)
        sql_area = "select vista_cinema_name,cinema_name,city,film_center from jycinema_info"
        df_area = read_sql(sql_area)
        df_table2 = pd.merge(left = df_table2,right = df_area,left_on = "影院",right_on = "vista_cinema_name",how = "left")
        df_table2.drop(columns = ["影院"],axis = 1,inplace = True)
        df_table2.rename(columns = {"影片":"film","已批准":"status_approve","已计划":"status_plan","开启":"status_open","总计":"status_total","cinema_name":"cinema"},inplace = True)
        df_table2 = df_table2.reindex(columns = ["cinema","city","film_center","film","status_open","status_plan","status_approve","status_total"])
        df_table2["op_date"] = date
        df_table2["fetch_date"] = str(today)
        to_sql(df_table2,"film_session_status")

#日志
set_logger = get_logger("/home/log/film_data","rt_session_status_statistic")

#清空历史数据
sql = "delete from film_session_status"
read_sql(sql)

startday = str(today)
endday = str(today + datetime.timedelta(days = 15))
delta_now = datetime.datetime.strptime(datetime.datetime.now().strftime("%H:%M:%S"),"%H:%M:%S") - datetime.datetime.strptime("06:00:00","%H:%M:%S")
#如果属于6点到23点59分
if delta_now.days != 0:
    startday = str(today - datetime.timedelta(days = 1))
    endday = str(today + datetime.timedelta(days = 16))

df = get_csv_data(startday)
datelist = pd.date_range(start = startday,end = endday)
for each_date in datelist:
    each_date = str(each_date)[:10]
    statistic_run(df,each_date)

set_logger.info("realtime session status statistic completed")
