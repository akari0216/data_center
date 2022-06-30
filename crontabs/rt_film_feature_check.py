import pandas as pd
import numpy as np
import pymysql
import ftplib
import re,os
import datetime
from sqlalchemy import create_engine
from logger import get_logger
import warnings

warnings.filterwarnings("ignore")

today = datetime.date.today()
#从数据库获取数据
def read_sql(sql):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset  = "utf8")
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

#对影片特征数进行统计
def film_feature_count(df,language_list,film_feature_dict):
    count_list = []
    for i in df.index:
        count = 0
        film_feature_list = str(df.loc[i,"场次特征"]).split("|")
        other_film_feature = list(film_feature_dict.values())
        for each_feat in film_feature_list:
            real_film_feature = str(df.loc[i,"影片特征"])
            if real_film_feature in other_film_feature:
                other_film_feature.remove(real_film_feature)
            if each_feat.strip() in real_film_feature or each_feat.strip() in other_film_feature or each_feat.strip() in language_list:
                count += 1
        count_list.append(count)

    df["特征计数"] = np.array(count_list)
    return df


#影片特征检查
def film_feature_check(df):
    language_list = ["国语","英语","日语","韩语","俄语","法语","德语","粤语","泰语","西班牙语","印度语","阿拉伯语","保加利亚语","波兰语","丹麦语","荷兰语","罗马尼亚语",\
        "蒙古语","闽南语","葡萄牙语","瑞典语","土耳其语","维吾尔语","西班牙语","意大利语","越南语","武汉话","四川方言","河南方言","湖北方言","湖南方言","陕西方言","泰米尔语"]
    film_feature_dict = {"数字":"数字2D","数字3D":"数字3D","数字IMAX":"IMAX2D","数字IMAX3D":"IMAX3D","CINITY":"CINITY2D,CINITY3D",\
        "杜比视界":"杜比视界2D,杜比视界3D","中国巨幕":"中国巨幕2D,中国巨幕3D"}
    field_dict = {"影厅":"cinema_hall","影片":"film","场次时间":"session_time","人数":"people","票房":"bo","场次状态":"session_status","总座位数":"seats","上座率":"occupancy",\
        "场次特征":"film_feature","专资日期":"op_date","cinema_name":"cinema"}
    pat = "(（.*?）)"
    df = df[df["场次状态"].isin(["开启"])]
    df["影片特征"] = df["影片"].str.extract(pat)
    df["影片特征"] = df["影片特征"].str.slice(1,-1).map(film_feature_dict)
    df = film_feature_count(df,language_list,film_feature_dict)
    #转换为专资时间，并将日期时间分开
    df["专资日期"] = (pd.to_datetime(df["场次时间"],format = "%Y-%m-%d") - datetime.timedelta(hours = 6)).astype(str).str.slice(0,10)
    df["场次时间"].replace("\d*?/\d*?/\d*?\s","",regex = True,inplace = True)
    #筛选出特征数异常的
    df = df[~df["特征计数"].isin([2])]
    #剔除一些列名
    df.drop(columns = ["数据获取时间","是否最新","影片特征","特征计数"],axis = 1,inplace = True)

    #匹配同城排片中心
    sql_area = "select vista_cinema_name,cinema_name,city,film_center from jycinema_info where op_status = 1"
    df_area = read_sql(sql_area)

    df = pd.merge(left = df,right = df_area,left_on = "影院",right_on = "vista_cinema_name",how = "left")
    df.drop(columns = ["影院","vista_cinema_name"],axis = 1,inplace = True)

    df.rename(columns = field_dict,inplace = True)
    df["fetch_date"] = str(today)
    df["occupancy"] = np.round(np.divide(df["people"].astype(float),df["seats"].astype(float),out = np.zeros_like(df["people"].astype(float)),where = df["seats"] != 0) * 100,2)
    df = df.reindex(columns = ["cinema","city","film_center","cinema_hall","film","session_time","people","bo","session_status","seats","occupancy","film_feature",\
        "op_date","fetch_date"])
    return df

set_logger = get_logger("/home/log/film_data","film_feature_check")

#先清除数据
sql = "delete from film_feature_check"
read_sql(sql)

#判断专资时间
df = pd.DataFrame()
deal_date = str(datetime.datetime.today() - datetime.timedelta(hours = 6))[:10]
if deal_date == str(today):
    df = get_csv_data(str(today))
elif deal_date == str(today - datetime.timedelta(days = 1)):
    df = get_csv_data(str(today - datetime.timedelta(days = 1)))

#获取数据写入数据库
df = film_feature_check(df)
to_sql(df,"film_feature_check")
set_logger.info("film_feature_check update completed!")
print("film_feature_check update completed!")