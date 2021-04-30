from sqlalchemy import create_engine
import pandas as pd
import datetime

def to_sql(df,tablename):
    conn = create_engine("mysql+pymysql://root:jy123456@192.168.16.114:3306/film_data?charset=utf8")
    df.to_sql(tablename,con = conn,if_exists = "append",index = False)

today = datetime.date.today()    
startday = today
endday = today + datetime.timedelta(days = 6) 

presale_df = pd.DataFrame(columns = ["fetch_date","presale_date"])
datelst = pd.date_range(start = startday,end = endday,freq = "D")
for each_day in datelst:
    presale_datelst = pd.date_range(start = each_day,end = each_day + datetime.timedelta(days = 2),freq = "D")
    for each_presale_date in presale_datelst:
        each_df = pd.DataFrame(data = {"fetch_date":each_day,"presale_date":each_presale_date},index = [0])
        presale_df = pd.concat([presale_df,each_df],ignore_index = True)
print(presale_df)
to_sql(presale_df,"presale_date_list")
