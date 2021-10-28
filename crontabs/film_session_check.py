import datetime
import os
import pymysql
import openpyxl
import smtplib
from smtplib import SMTPException
import pandas as pd
import numpy as np
import re
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formataddr

path = "/home/project/data_center/crontabs/film_check"
os.chdir(path)

# today = datetime.date.today()
today = datetime.date.today()
tomorrow = today + datetime.timedelta(days = 1)
third_day = today + datetime.timedelta(days = 2)
field_dict = {"cinema_name":"影城名","city":"同城","film_center":"排片中心","film":"影片名","presale_date":"预售日期"}

def read_sql(sql):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
    df = pd.read_sql(sql,conn)
    return df

def film_session(top_film=3,add_date=[]):
    os.chdir(path)
    field_list = ["cinema_name","city","film_center","film","presale_date"]
    df_total = pd.DataFrame(columns = field_list)
    #所有影城
    sql_cinema = "select cinema_name,city,film_center from jycinema_info where op_status = 1"
    df_cinema = read_sql(sql_cinema)
    df_cinema["val"] = 1
    #疫情停业影城
    sql_suspend_cinema = "select cinema_name from suspend_cinema_list"
    suspend_cinema_list = read_sql(sql_suspend_cinema)["cinema_name"].tolist()
    df_cinema = df_cinema[~df_cinema["cinema_name"].isin(suspend_cinema_list)]
    
    datelist = [str(x)[:10] for x in pd.date_range(start=str(tomorrow),end=str(third_day),freq="D")]
    if len(add_date) != 0:
        datelist.extend(add_date)
    for each_date in datelist:
        print(each_date)
        #得到场次数最多的影片
        sql_film = "select film,sum(session) as total_session from film_cinema where op_date = '%s' and fetch_date = '%s' group by film order by total_session desc" % (each_date,today)
        df_film = read_sql(sql_film).iloc[:top_film,:]
        df_film["val"] = 1
        df_film.drop(columns = ["total_session"],axis = 1,inplace = True)
        #所有影城影片
        df_total_cinema_film = pd.merge(left = df_cinema,right = df_film,on = "val",how = "left")
        df_total_cinema_film["match"] = df_total_cinema_film["cinema_name"] + df_total_cinema_film["film"]
#         print(df_total_cinema_film)
        #得到当天影城影片
        sql_cinema_film = "select cinema,film,presale_date from presale_film_cinema where presale_date = '%s' and fetch_date = '%s'" % (each_date,today)
        df_cinema_film = read_sql(sql_cinema_film)
        df_cinema_film["match"] = df_cinema_film["cinema"] + df_cinema_film["film"]
#         print(df_cinema_film)
        match_list = df_cinema_film["match"].tolist()
        df_total_cinema_film = df_total_cinema_film[~df_total_cinema_film["match"].isin(match_list)]
#         print(df_total_cinema_film)
        if len(df_total_cinema_film) != 0:
            df_total_cinema_film.drop(columns = ["val","match"],axis = 1,inplace = True)
            df_total_cinema_film["presale_date"] = each_date
            df_total = pd.concat([df_total,df_total_cinema_film],ignore_index=True)
    
    df_total.sort_values(by = ["film_center","presale_date","cinema_name"],ascending=[True,True,True],inplace=True)
    df_total.rename(columns = field_dict,inplace = True)    
    print(df_total)
    file_name = "实时影城未排影片表.xlsx"
    df_total.to_excel(file_name)
    return file_name


#邮箱登录
def mail_login():
    smtpserver = "smtp.exmail.qq.com"
    username = "xxsjfxyj@jycinema.com"
    password = "jy314159@DATA"
    sender = "xxsjfxyj@jycinema.com" 
    smtp = smtplib.SMTP_SSL(smtpserver,465)
    smtp.ehlo()
    smtp.login(user = username,password = password)
    return smtp,sender

#发送邮件
def sendmail(smtp,sender,receiver,file):
    os.chdir(path)
    print("sending cinema mail")
    send_status = ""
    msg = MIMEMultipart()
    msg["Subject"] = file.rstrip(".xlsx")
    msg["From"] = formataddr(["数据分析研究中心",sender])
    msg["To"] = receiver
    text_details = file.strip(".xlsx")
    text = MIMEText(text_details)
    msg.attach(text)
    #excel内容
    att = MIMEApplication(open(file,"rb").read())
    att.add_header("Content-Disposition","attachment",filename = ("GBK","",file))
    msg.attach(att)
    #进行发送
    try:
        if "," in receiver:
            smtp.sendmail(sender,receiver.split(","),msg.as_string())
        else:
            smtp.sendmail(sender,receiver,msg.as_string())
        send_status = "success"
    except SMTPException as e:
        send_status = "fail"

def get_mail_addr():
    sql = "select mail_addr from film_check_mail_addr"
    mail_addr_list = read_sql(sql)["mail_addr"].tolist()
    return mail_addr_list

def clear_data(path):
    file_list = os.listdir(path)
    for each_file in file_list:
        os.remove(each_file)

if __name__ == "__main__":
    smtp,sender = mail_login()
    mail_list = get_mail_addr()
    mail_list = str(mail_list).strip('[').strip(']')
    film_session_check_xlsx = film_session()
    sendmail(smtp,sender,mail_list,film_session_check_xlsx)
    
    

