from sshtunnel import SSHTunnelForwarder
import pandas as pd
import datetime
import pymysql
from sqlalchemy import create_engine
from logger import get_logger

today = datetime.date.today()
yesterday = str(today - datetime.timedelta(days = 1))
#判断专资时间
deal_date = str(datetime.datetime.today() - datetime.timedelta(hours = 6))[:10]

def ssh_port():
    server = SSHTunnelForwarder(
    ssh_address_or_host = ("1.12.243.7",22),
    ssh_username = "root",
    ssh_password = "JinYi2017*",
    remote_bind_address =  ("localhost",3306))
    server.start()

    return server,server.local_bind_port

#可以通过port定义ssh连接
def connect_to_db(port = 3306):
    conn = pymysql.connect(
        user = "root",
        passwd = "jy123456",
        host = "localhost",
        db = "film_data",
        port = port)

    return conn

def update_data_to_ssh(df,tablename,port):
    conn = create_engine("mysql+pymysql://root:jy123456@127.0.0.1:%s/film_data?charset=utf8" % port)
    df.to_sql(tablename,con = conn,if_exists = "append",index = False)

def get_df(sql,conn):
    res = pd.read_sql(sql,conn)
    df = pd.DataFrame(res)
    return df

if __name__ == "__main__":
    server,server_port = ssh_port()
    conn_local = connect_to_db()
    conn_server = connect_to_db(port = server_port)
    cursor_server = conn_server.cursor()
    logger = get_logger("/home/log/film_data","update_ssh_data")
    #获取本地当日数据
    df_film_cinema_sql = "select * from film_cinema where fetch_date = '%s' and op_date = '%s'" % (str(today),yesterday)
    df_film_cinema = get_df(df_film_cinema_sql,conn_local)
    #清除远程当日数据
    cursor_server.execute("delete from film_cinema where fetch_date = '%s'" % (str(today)))
    #向远程写入当日数据
    update_data_to_ssh(df_film_cinema,"film_cinema",conn_server)
    server.stop()
    print("update lastday data to ssh success")
    logger.info("update lastday data to ssh success")
