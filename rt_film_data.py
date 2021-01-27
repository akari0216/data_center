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
def get_csv_data(datelist):
    df_total = pd.DataFrame()
    time_str_list = []
    #flag用于标记两个日期是否均大于等于当天
    flag = 0
    ftp = ftplib.FTP()
    ftp.connect(host = "172.20.240.195",port = 21 ,timeout = 30)
    ftp.login(user = "sjzx",passwd = "jy123456@")
    ftp.set_pasv(False)
    #加入判定条件，在20200805之前的日期需进入到reexport目录
    for each_date in datelist:
        each_date = each_date.replace("-","")
        #若为当天，需要再判断
        today_str = str(tyear) + str(tmonth) + str(tday)
        if each_date == today_str:
            flag += 1
            #判断当前时间是否属于0-6点
            delta_now = datetime.datetime.strptime(datetime.datetime.now().strftime("%H:%M:%S"),"%H:%M:%S") - datetime.datetime.strptime("06:00:00","%H:%M:%S")
            #如果属于6点到23点59分
            if delta_now.days == 0:
                each_date = today
            #如果属于0点到5点59分
            else:
                each_date = today - datetime.timedelta(days = 1)
        elif each_date > today_str:
            flag += 1
            each_date = today
        if flag != 2:
            each_date = str(each_date).replace("-","")
            filename = "SessionRevenue_"+ each_date +".csv"
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
                    datelist = sorted(df["场次时间"].str.slice(0,10).drop_duplicates().tolist())
                    df_total = pd.concat([df_total,df],ignore_index = True)
            
    ftp.quit()
    return df_total,datelist

#初步处理数据
def process_data(datelist):
    df_list = []
    def data_filter(df,time1,time2):
        if len(df) != 0:
            timelist = []
            showcount_time = df["场次时间"]
            for each_time in showcount_time:
                tmp_time = datetime.datetime.strptime(each_time,"%Y-%m-%d %H:%M:%S")
                delta1 = tmp_time - time1
                delta2 = time2 - tmp_time
                if delta1.days == 0 and delta2.days == 0:
                    timelist.append(each_time)
            df = df[df["场次时间"].isin(timelist)]
            pat = "（.*?）\s*|\(.*?\)\s*|\s*"
            df["影片"].replace(pat,"",regex = True,inplace = True)
            return df    
    
    df,time_str_list = get_csv_data(datelist)
    #选择当天的csv文件，然后按照datelist
    for each_date in datelist:
        t1 = datetime.datetime.strptime(str(each_date) + " 06:00:00","%Y-%m-%d %H:%M:%S")
        each_date = datetime.date(int(each_date[0:4]),int(each_date[5:7]),int(each_date[8:]))
        t2 = datetime.datetime.strptime(str(each_date + datetime.timedelta(days = 1)) + " 05:59:59","%Y-%m-%d %H:%M:%S")
        each_df = data_filter(df,t1,t2)
        df_list.append(each_df)
    
    return df_list,time_str_list

center_list = ["南排片中心","北排片中心"]
city_list = ["广州同城","中山同城","佛山同城","福州同城","厦门同城","深莞同城","长沙同城","粤西同城","武汉同城","北京同城","上海同城","天津同城","杭州同城","苏州同城",\
             "南京同城","常州同城","重庆同城","辽宁同城","山东同城","合肥同城"]
field_dict = {"影城":"cinema","同城":"city","排片中心":"film_center","影片":"film","场次":"session","场次占比":"session_percent","人次":"people","人次占比":"people_percent",\
              "总座位数":"seats","排座占比":"seats_percent","票房":"bo","票房占比":"bo_percent","金逸供需":"jy_ratio","排座效率":"arrange_film_effect","排座效益":"arrange_film_benefit","人均票价":"avg_price",\
              "上座率":"occupancy","场均人次":"people_per_session","平均票价":"avg_price","数据日期":"op_date","获取日期":"fetch_date","影厅":"hall","场次时间":"session_time","场次状态":"session_status","座位数":"seats"}

#按照列表顺序重排dataframe
def reorder_df(df,field_list,field):
    df_total = pd.DataFrame(columns = df.columns)
    for each_field in field_list:
        each_df = df[df[field].isin([each_field])]
        #按场次降序
        each_df.sort_values(by = "场次",ascending = False,inplace = True)
        df_total = pd.concat([df_total,each_df],ignore_index = True)
        
    return df_total

#统一零除错误
def df_divide(df_field1,df_field2):
    return np.divide(df_field1,df_field2,out = np.zeros_like(df_field1),where = df_field2 != 0)

#对已初步处理的数据进行透视并计算
def pivot_data(df,date):
    if df is not None:
        df = df[df["场次状态"].isin(["开启","已计划","已批准"])]
        df1 = df.copy()
        df1["场次时间"].replace(" \d\d:\d\d:\d\d","",regex = True,inplace = True)
        df2 = df.copy()
        df2["场次时间"].replace("\d\d\d\d-\d\d-\d\d ","",regex = True,inplace = True)
        df2["场次时间"] = df2["场次时间"].str.slice(0,5)
        #构造生成前4个sheet的方法
        def arrange_film_cal(df_cal,field_list,sheet_name,sql = "",sql_field = [],list1= []):
            df_cal_sql = ""
            if sql != "" and len(sql_field) != 0:
                df_cal_sql = read_sql(sql).loc[:,sql_field]
                df_cal = pd.merge(left = df_cal,right = df_cal_sql,left_on = "影院",right_on = sql_field[0],how = "left")
                df_cal_sql = df_cal_sql.loc[:,sql_field[1:]]
                df_cal_sql.drop_duplicates(sql_field[1:],keep = "first",inplace = True)
                #field_list = ["同城","影片"]，sql_field = ["cinema_name_old","city"]
                df_cal.rename(columns = {"cinema_name":"影城","city":"同城","film_center":"排片中心"},inplace = True)
            table = pd.pivot_table(df_cal,index = [field_list[0]],values = ["上座率","人数","总座位数","票房"],aggfunc = {"上座率":len,"人数":np.sum,"总座位数":np.sum,"票房":np.sum},fill_value = 0,margins = False)
            table.reset_index(inplace = True)
            df_table = pd.DataFrame(table)
            df_table.rename(columns = {"上座率":"场次","人数":"人次"},inplace = True)
            if len(field_list) != 1:
                col_dict = {"票房":"总票房","场次":"总场次","人次":"总人次","总座位数":"总总座位数"}
                df_table.rename(columns = col_dict,inplace = True)
                table2 = pd.pivot_table(df_cal,index = field_list,values = ["上座率","人数","总座位数","票房"],aggfunc = {"上座率":len,"人数":np.sum,"总座位数":np.sum,"票房":np.sum},fill_value = 0,margins = False)
                table2.reset_index(inplace = True)
                df_table2 = pd.DataFrame(table2)
                df_table2.rename(columns = {"上座率":"场次","人数":"人次"},inplace = True)
                df_table = pd.merge(left = df_table2,right = df_table,how = "left",on = field_list[0])
                df_table["场次占比"] = np.round(df_table["场次"] / df_table["总场次"] * 100,2)
                df_table["人次占比"] = np.round(df_table["人次"] / df_table["总人次"] * 100,2)
                df_table["排座占比"] = np.round(df_table["总座位数"] / df_table["总总座位数"] * 100,2)
                df_table["票房占比"] = np.round(df_table["票房"] / df_table["总票房"] * 100,2)
            else:
                df_table["场次占比"] = np.round(df_table["场次"] / df_table["场次"].sum() * 100,2)
                df_table["人次占比"] = np.round(df_table["人次"] / df_table["人次"].sum() * 100,2)
                df_table["排座占比"] = np.round(df_table["总座位数"] / df_table["总座位数"].sum() * 100,2)
                df_table["票房占比"] = np.round(df_table["票房"] / df_table["票房"].sum() * 100,2)
            tmp_list = field_list.copy()
            tmp_list.extend(["场次","场次占比","人次","人次占比","总座位数","排座占比","票房","票房占比"])
            df_table = df_table.reindex(columns = tmp_list)
            df_table["金逸供需"] = np.round(df_divide(df_table["票房占比"],df_table["场次占比"]),2)
            df_table["排座效率"] = np.round(df_divide(df_table["人次占比"],df_table["排座占比"]),2)
            df_table["排座效益"] = np.round(df_divide(df_table["票房占比"],df_table["排座占比"]),2)
            if len(field_list) != 1:
                df_table["上座率"] = np.round(df_divide(df_table["人次"].astype(float),df_table["总座位数"].astype(float)) * 100,2)
                df_table["场均人次"] = np.round(df_divide(df_table["人次"].astype(float),df_table["场次"].astype(float)),2)
                df_table["平均票价"] = np.round(df_divide(df_table["票房"].astype(float),df_table["人次"].astype(float)),2)
                df_table = reorder_df(df_table,list1,field_list[0])
                #匹配同城及排片中心，并插入到第一列的后边
                if len(sql_field) > 2:
                    df_table = pd.merge(left = df_table,right = df_cal_sql,left_on = field_list[0],right_on = sql_field[1],how = "left")
                    df_table.drop(columns = sql_field[1],axis = 1,inplace = True)
                    df_table_col = df_table.columns.tolist()
                    tmp_col = df_table_col[1:]
                    df_table_col = df_table_col[0:1]
                    df_table_col.extend(sql_field[2:])
                    df_table_col.extend(tmp_col)
                    for i in range(len(sql_field[2:])):
                        df_table_col.pop()
                    df_table = pd.DataFrame(df_table,columns = df_table_col)
                    df_table.rename(columns = {"cinema_name":"影城","city":"同城","film_center":"排片中心"},inplace = True)
            else:
                df_table["上座率"] = np.round((df_table["人次"] / df_table["总座位数"] * 100),2)
                df_table["场均人次"] = np.round((df_table["人次"] /df_table["场次"]),2)
                df_table["平均票价"] = np.round((df_table["票房"] / df_table["人次"]),2)
                df_table.sort_values(by = "场次",ascending = False,inplace = True)
            
            df_table["数据日期"] = str(date)
            df_table["获取日期"] = str(today)
            df_table.rename(columns = field_dict,inplace = True)
            return df_table
        
        res1 = arrange_film_cal(df1,["影片"],"总部数据")
        to_sql(res1,"film_total")
        sql = "select vista_cinema_name,cinema_name,city,film_center from jycinema_info"
        sql_center_field = ["vista_cinema_name","film_center"]
        res2 = arrange_film_cal(df1,["排片中心","影片"],"排片中心",sql,sql_center_field,center_list)
        to_sql(res2,"film_center")
        sql_city_field = ["vista_cinema_name","city","film_center"]
        res3 = arrange_film_cal(df1,["同城","影片"],"同城",sql,sql_city_field,city_list)
        to_sql(res3,"film_city")
        sql_cinema_field = ["vista_cinema_name","cinema_name","city","film_center"]
        cinema_list = read_sql(sql).loc[:,"cinema_name"].tolist()
        res4 = arrange_film_cal(df1,["影城","影片"],"影城",sql,sql_cinema_field,cinema_list)
        to_sql(res4,"film_cinema")
        
        df_sql = read_sql(sql)
        sql_cinema_field = ["vista_cinema_name","cinema_name","city","film_center"]
        df_sql = df_sql.loc[:,sql_cinema_field]
        df2 = pd.merge(left = df2,right = df_sql,left_on = "影院",right_on = "vista_cinema_name",how = "left")
        df2.drop(columns = ["影院"],axis = 1,inplace = True)
        df2.rename(columns = {"cinema_name":"影城","city":"同城","film_center":"排片中心","总座位数":"座位数","人数":"人次"},inplace = True)
        df2["人均票价"] = np.round(df_divide(df2["票房"],df2["人次"]),2)
        df2["上座率"] = np.round(df2["人次"] / df2["座位数"] *100,2).fillna(0)
        df2["数据日期"] = str(date)
        df2["获取日期"] = str(today)
        df2 = df2.reindex(columns = ["影城","同城","排片中心","影厅","影片","场次时间","票房","人次","人均票价","座位数","上座率","场次状态","数据日期","获取日期"])
        df2.sort_values(by = ["排片中心","同城","影城","影厅","场次时间"],ascending = [False,False,False,True,True],inplace = True)
        df2.rename(columns = field_dict,inplace = True)
        to_sql(df2,"film_session_detail")

set_logger = get_logger("/home/log/film_data","rt_film_data")
table_list = ["film_total","film_center","film_city","film_cinema","film_session_detail"]

#判断专资时间
deal_date = str(datetime.datetime.today() - datetime.timedelta(hours = 6))[:10]

yesterday = str(today - datetime.timedelta(days = 1))
conn = pymysql.connect(host = "192.168.16.114",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
cursor = conn.cursor()
for each_table in table_list:
    #若为6-24点
    if deal_date == str(today):
        cursor.execute("delete from %s where fetch_date = '%s' and op_date >= '%s'" % (each_table,str(today),str(today)))
        set_logger.info("delete table %s fetch_date %s and op_date >= '%s' completed" % (each_table,str(today),str(today)))

    #否则为0-6点
    elif deal_date == yesterday:
        cursor.execute("delete from %s where fetch_date in ('%s','%s') and op_date >= '%s'" % (each_table,yesterday,str(today),yesterday))
        set_logger.info("delete table %s fetch_date in ('%s','%s') and op_date >= '%s' completed" % (each_table,yesterday,str(today),yesterday))

df_list,datelist = process_data([deal_date])
for each_date in datelist:
    print(each_date)
    pivot_data(df_list[0],each_date)

set_logger.info("realtime film data completed")
print("rt film data completed %s" % deal_date)
cursor.close()
conn.close()
