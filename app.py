import os
import click
from flask import Flask,request,render_template,url_for,redirect,session
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_
from flask_migrate import Migrate
import json
import pymysql
from jinja2 import escape
import decimal
import datetime
import pandas as pd
from blueprints.admin import admin_blue
from blueprints.city import city_blue
from blueprints.cinema import cinema_blue

from random import randrange
from pyecharts import options as opts
from pyecharts.charts import Bar,Line
from pyecharts.faker import Faker
from pyecharts import options as opts

app = Flask(__name__,static_folder = "",static_url_path = "")
app.secret_key = os.getenv("SECRET KEY","secret string")
app.jinja_env.trim_blocks = True
app.jinja_lstrip_block = True
app.register_blueprint(admin_blue)
app.register_blueprint(city_blue)
app.register_blueprint(cinema_blue)

#数据库连接
app.config["SQLALCHEMY_DATABASE_URI"] = "mysql+pymysql://root:jy123456@localhost:3306/film_data"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
migrate = Migrate(app,db)

#查询表-字段映射
fields_dict = {"film_total":"film,session,session_percent,people,people_percent,seats,seats_percent,bo,bo_percent,jy_ratio,arrange_film_effect,\
        arrange_film_benefit,occupancy,people_per_session,op_date",\
            "film_center":"film_center,film,session,session_percent,people,people_percent,seats,seats_percent,bo,bo_percent,jy_ratio,arrange_film_effect,\
        arrange_film_benefit,occupancy,people_per_session,op_date",\
            "film_city":"city,film_center,film,session,session_percent,people,people_percent,seats,seats_percent,bo,bo_percent,jy_ratio,arrange_film_effect,\
        arrange_film_benefit,occupancy,people_per_session,op_date",\
            "film_cinema":"cinema,city,film_center,film,session,session_percent,people,people_percent,seats,seats_percent,bo,bo_percent,jy_ratio,arrange_film_effect,\
        arrange_film_benefit,occupancy,people_per_session,op_date",\
            "film_session_detail":"cinema,city,film_center,hall,film,session_time,bo,people,avg_price,seats,occupancy,session_status,op_date",
            "film_session_status":"cinema,city,film_center,film,status_open,status_plan,status_approve,status_total,op_date",
            "red_film_data":"cinema,city,film_center,sum(red_film_session) as red_film_session",
            "red_film_abnormal":"cinema,city,film_center,hall,film,session_time,bo,people,seats,occupancy,session_status,op_date",
            "not_open_film_cinema":"cinema,city,film_center,film,op_date",
            "predict_film_cinema":"cinema,city,film_center,film,predict_bo,predict_bo_percent,predict_date",
            "film_feature_check":"cinema,city,film_center,cinema_hall,film,session_time,people,bo,seats,occupancy,session_status,film_feature,op_date"}

#字段映射2
fields_dict2 = {"session_percent":"场次占比","people_percent":"人次占比","seats_percent":"排座占比","bo_percent":"票房占比","arrange_film_effect":"排座效率","arrange_film_benefit":"排座效益",\
    "people_per_session":"场均人次","occupancy":"上座率","avg_price":"平均票价"}

#如何转为数据存储？
class UserAccount(db.Model):
    id = db.Column(db.Integer,primary_key = True)
    username = db.Column(db.String(length = 20))
    password = db.Column(db.String(length = 20))
    # usermessage = db.relationship("UserMessage",back_populates = "useraccount")

class UserMessage(db.Model):
    id = db.Column(db.Integer,primary_key = True)
    username = db.Column(db.String(length = 20))
    usermsg = db.Column(db.String(length = 20))
    user_auth_role = db.Column(db.String(length = 20))
    user_auth_area = db.Column(db.String(length = 20))

class UpdateTimelist(db.Model):
    id = db.Column(db.Integer,primary_key = True)
    fetch_date = db.Column(db.Date())
    update_time = db.Column(db.String(length = 20))

#json编码转换
class encoder(json.JSONEncoder):
    def default(self,obj):
        if isinstance(obj,decimal.Decimal):
            return float(obj)
        elif isinstance(obj,datetime.date):
            return obj.strftime("%Y-%m-%d")
        elif isinstance(obj,datetime.timedelta):
            return str(obj)
        else:
            return obj.JSONEncoder.default(self,obj)

#登录
@app.route("/login",methods = ["GET","POST"])
def login():
    wrong_flag = 0
    session["auth"] = ""
    session["usermsg"] = ""
    data = request.args.to_dict()
    if "account" in data.keys() and "password" in data.keys():
        account = data["account"]
        passwd = data["password"]
        if UserAccount.query.filter(and_(UserAccount.username == account,UserAccount.password == passwd)).all():
            usermsg = UserMessage.query.filter(UserMessage.username == account).first().usermsg
            user_auth_role = UserMessage.query.filter(UserMessage.username == account).first().user_auth_role
            user_auth_area = UserMessage.query.filter(UserMessage.username == account).first().user_auth_area
            print("account",account,"passwd",passwd)
            print("user_auth_role",user_auth_role,"user_auth_area",user_auth_area)
            session["auth"] = "authenrized"
            session["usermsg"] = usermsg
            session["user_auth_role"] = user_auth_role
            session["user_auth_area"] = user_auth_area
            # 以session取代账号密码
            data.pop("account")
            data.pop("password")
            #按权限级别划分
            if user_auth_role == "admin":
                return redirect(url_for("admin.table_list"))
            elif user_auth_role == "city":
                return redirect(url_for("city.table_list"))
            elif user_auth_role == "cinema":
                return redirect(url_for("cinema.table_list"))
        else:
            wrong_flag = 1

    return render_template("login.html",wrong_flag = wrong_flag)

#主页
@app.route("/home")
def home():
    return render_template("home.html")


#测试展示图
def line_chart(field,fetch_date_list,data_list) -> Line:
    line = Line()
    line.add_xaxis(fetch_date_list)
    for each_data in data_list:
        line.add_yaxis(each_data[0],each_data[1],linestyle_opts = opts.LineStyleOpts(width = 1.5))
    if field in ["session_percent","people_percent","seats_percent","bo_percent","occupancy"]:
        line.set_global_opts(yaxis_opts = opts.AxisOpts(name= fields_dict2[field],axislabel_opts = opts.LabelOpts(formatter = "{value} %")),tooltip_opts = opts.TooltipOpts(trigger = "axis",axis_pointer_type = "cross"),\
                datazoom_opts = opts.DataZoomOpts(range_start = 0,range_end = 100))
    elif field == "avg_price":
        line.set_global_opts(yaxis_opts = opts.AxisOpts(name= fields_dict2[field],axislabel_opts = opts.LabelOpts(formatter = "{value} 元")),tooltip_opts = opts.TooltipOpts(trigger = "axis",axis_pointer_type = "cross"),\
                datazoom_opts = opts.DataZoomOpts(range_start = 0,range_end = 100))
    else:
        line.set_global_opts(yaxis_opts = opts.AxisOpts(name= fields_dict2[field],axislabel_opts = opts.LabelOpts(formatter = "{value}")),tooltip_opts = opts.TooltipOpts(trigger = "axis",axis_pointer_type = "cross"),\
                datazoom_opts = opts.DataZoomOpts(range_start = 0,range_end = 100))

    return line

@app.route("/lineChart")
def get_line_chart():
    get_data = request.args.to_dict()
    chart_table = get_data.get("chart_table")
    field_val = get_data.get("field_val")
    date = get_data.get("date")
    area_field = ""
    area_value = ""
    if chart_table != "" and field_val != "" and date != "":
        if "area_field" in get_data.keys() and "area_value" in get_data.keys():
            area_field = get_data.get("area_field")
            area_value = get_data.get("area_value")
        fetch_date_list,data_list = sql_chart(chart_table,field_val,area_field,area_value,date)
        c = line_chart(field_val,fetch_date_list,data_list)
        return c.dump_options_with_quotes()

#报表数据接口
@app.route("/data/api")
def api():
    return_dict = {"code":0,"msg":"处理成功","result":False,"total":0}
    if request.args is None:
        return_dict["return_code"] = 504
        return_dict["return_info"] = "请求参数为空"
        return json.dumps(return_dict,ensure_ascii = False,cls = encoder)

    date2 = 0
    get_data = request.args.to_dict()
    date = get_data.get("date")
    table = get_data.get("table")
    if "date2" in get_data.keys():
        date2 = get_data.get("date2")
    area_field = ""
    area_value = ""
    film = ""
    page = 1
    limit = 30
    if "page" in get_data.keys() and "limit" in get_data.keys() and "table" in get_data.keys():
        page = int(get_data.get("page"))
        limit = int(get_data.get("limit"))
        if "area_field" in get_data.keys() and "area_value" in get_data.keys():
            area_field = get_data.get("area_field")
            area_value = get_data.get("area_value")
        if "film" in get_data.keys():
            film = get_data.get("film")
    return_dict["result"],return_dict["total"] = sql_result(table,area_field,area_value,date,date2,film,page,limit)

    return json.dumps(return_dict,ensure_ascii = False,cls = encoder)

#sql数据查询器
def sql_result(table,area_field,area_value,date,date2,film,page,limit):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
    cursor = conn.cursor()
    length = 0
    #date2用以判断为单日期选择还是日期段选择
    if date2 == 0:
        if area_value != "":
            #影片查询只对排片中心/同城作用
            if film != "":
                cursor.execute("select count(*) from %s where op_date = '%s' and %s = '%s' and film = '%s'" % (table,date,area_field,area_value,film))
                length = cursor.fetchall()[0][0]
                cursor.execute("select %s from %s where op_date = '%s' and %s = '%s' and film = '%s' limit %d,%d" % (fields_dict[table],table,date,area_field,area_value,film,(page - 1) * limit,limit))
            else:
                cursor.execute("select count(*) from %s where op_date = '%s' and %s = '%s'" % (table,date,area_field,area_value))
                length = cursor.fetchall()[0][0]
                if table == "film_session_detail" or table == "film_session_status" or table == "not_open_film_cinema":
                    cursor.execute("select %s from %s where op_date = '%s'  and %s = '%s' limit %d,%d" % (fields_dict[table],table,date,area_field,area_value,(page - 1) * limit,limit))
                else:
                    cursor.execute("select %s from %s where op_date = '%s'  and %s = '%s' order by session desc limit %d,%d" % (fields_dict[table],table,date,area_field,area_value,(page - 1) * limit,limit))
        else:
            if film != "":
                cursor.execute("select count(*) from %s where op_date = '%s' and film = '%s'" % (table,date,film))
                length = cursor.fetchall()[0][0]
                cursor.execute("select %s from %s where op_date = '%s' and film = '%s' limit %d,%d" % (fields_dict[table],table,date,film,(page - 1) * limit,limit))
            else:
                cursor.execute("select count(*) from %s where op_date = '%s'" % (table,date))
                length = cursor.fetchall()[0][0]
                if table == "film_session_detail" or table == "film_session_status" or table == "not_open_film_cinema":
                    cursor.execute("select %s from %s where op_date = '%s' limit %d,%d" % (fields_dict[table],table,date,(page - 1) * limit,limit))
                else:
                    cursor.execute("select %s from %s where op_date = '%s' order by session desc limit %d,%d" % (fields_dict[table],table,date,(page - 1) * limit,limit))
    else:
        if table != "red_film_data":
            if area_value != "":
                cursor.execute("select count(*) from %s where op_date >= '%s' and op_date <= '%s' and %s = '%s'" % (table,date,date2,area_field,area_value))
                length = cursor.fetchall()[0][0]
                cursor.execute("select %s from %s where op_date >= '%s' and op_date <= '%s' limit %d,%d" % (fields_dict[table],table,date,date2,(page - 1) * limit,limit))
            else:
                cursor.execute("select count(*) from %s where op_date >= '%s' and op_date <= '%s'" % (table,date,date2))
                length = cursor.fetchall()[0][0]
                cursor.execute("select %s from %s where op_date >= '%s' and op_date <= '%s' limit %d,%d" % (fields_dict[table],table,date,date2,(page - 1) * limit,limit))
        else:
            if area_value != "":
                cursor.execute("select %s from %s where op_date >= '%s' and op_date <= '%s' and %s = '%s' group by cinema" % (fields_dict[table],table,date,date2,area_field,area_value))
                length = len(cursor.fetchall())
                cursor.execute("select %s from %s where op_date >= '%s' and op_date <= '%s' and %s = '%s' group by cinema limit %d,%d" % (fields_dict[table],table,date,date2,area_field,area_value,(page - 1) * limit,limit))
            else:
                cursor.execute("select %s from %s where op_date >= '%s' and op_date <= '%s' group by cinema" % (fields_dict[table],table,date,date2))
                length = len(cursor.fetchall())
                cursor.execute("select %s from %s where op_date >= '%s' and op_date <= '%s' group by cinema limit %d,%d" % (fields_dict[table],table,date,date2,(page - 1) * limit,limit))
        
    result = cursor.fetchall()
    fields = cursor.description
    conn.close()
    res_lst = []
    fields_lst = []
    for each_field in fields:
        fields_lst.append(each_field[0])
    for each_res in result:
        each_res = user_auth_area_data_filter(each_res)
        if each_res is not None:
            res_lst.append(dict(zip(fields_lst,each_res)))
    return res_lst,length
    

#区域划分api
@app.route("/data/area/api")
def area_api():
    return_dict = {"code":0,"msg":"处理成功","result":False}
    if request.args is None:
        return_dict["return_code"] = 504
        return_dict["return_info"] = "请求参数为空"
        return json.dumps(return_dict,ensure_ascii = False,cls = encoder)
    get_data = request.args.to_dict()
    if "area_field" in get_data.keys() and "area_value" in get_data.keys():
        area_field = get_data.get("area_field")
        area_value = get_data.get("area_value")
        return_dict["result"] = sql_area_list(area_field,area_value)

    return json.dumps(return_dict,ensure_ascii = False,cls = encoder)

#返回同城或影城列表
def sql_area_list(area_field,area_value):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
    cursor = conn.cursor()
    if area_field == "film_center" and area_value != "":
        cursor.execute("select distinct(city) from jycinema_info where film_center='%s'" % area_value)
    elif area_field == "city" and area_value != "":
        cursor.execute("select distinct(cinema_name) from jycinema_info where city='%s' and op_status = 1" % area_value)
    result = cursor.fetchall()
    conn.close()
    res_lst = []
    for each_res in result:
        res_lst.append(each_res[0])
    return res_lst

#影片列表查询
@app.route("/data/film/api")
def film_api():
    return_dict = {"code":0,"msg":"处理成功","result":False}
    if request.args is None:
        return_dict["return_code"] = 504
        return_dict["return_info"] = "请求参数为空"
        return json.dumps(return_dict,ensure_ascii = False,cls = encoder)
    get_data = request.args.to_dict()
    area_field = ""
    area_value = ""
    date = get_data.get("date")
    table = get_data.get("table")
    if "area_field" in get_data.keys() and "area_value" in get_data.keys():
        area_field = get_data.get("area_field")
        area_value = get_data.get("area_value")
    return_dict["result"] = sql_film_list(date,table,area_field,area_value)
    
    return json.dumps(return_dict,ensure_ascii = False,cls = encoder)

#返回排场前10影片列表
def sql_film_list(date,table,area_field,area_value):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
    cursor = conn.cursor()
    order_val = "session"
    if table == "film_session_status":
        order_val = "status_total"
    if area_value == "":
        cursor.execute("select distinct(film) from %s where op_date = '%s' order by %s desc limit 10" % (table,date,order_val))
    else:
        if table == "maoyan_presale_price":
            cursor.execute("select distinct(movie_name) from %s where show_date = '%s' and jycinema_name = '%s'" % (table,date,area_value))
        else:
            cursor.execute("select distinct(film) from %s where op_date = '%s' and %s = '%s' order by %s desc limit 10" % (table,date,area_field,area_value,order_val))

    result = cursor.fetchall()
    conn.close()
    res_lst = []
    for each_res in result:
        res_lst.append(each_res[0])
    return res_lst

#影片模糊查询列表
@app.route("/data/fuzzy_film/api")
def fuzzy_film_api():
    return_dict = {"code":0,"msg":"处理成功","result":False}
    if request.args is None:
        return_dict["return_code"] = 504
        return_dict["return_info"] = "请求参数为空"
        return json.dumps(return_dict,ensure_ascii = False,cls = encoder)
    get_data = request.args.to_dict()
    area_field = ""
    area_value = ""
    date = get_data.get("date")
    table = get_data.get("table")
    kw = get_data.get("kw")
    if "area_field" in get_data.keys() and "area_value" in get_data.keys():
        area_field = get_data.get("area_field")
        area_value = get_data.get("area_value")
    return_dict["result"] = sql_fuzzy_film_list(date,table,area_field,area_value,kw)

    return json.dumps(return_dict,ensure_ascii = False,cls = encoder)

#注意权限分配
#返回关键字模糊查询影片
def sql_fuzzy_film_list(date,table,area_field,area_value,kw):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
    cursor = conn.cursor()
    if area_field == "" and area_value == "":
        print("select distinct(film) from %s where film like '%%%s%%' and op_date = '%s'" % (table,kw,date))
        cursor.execute("select distinct(film) from %s where film like '%%%s%%' and op_date = '%s'" % (table,kw,date))
    elif area_field != "" and area_field != "":
        print("select distinct(film) from %s where %s = %s and film like '%%%s%%' and op_date = '%s'" % (table,area_field,area_value,kw,date))
        cursor.execute("select distinct(film) from %s where %s = '%s' and film like '%%%s%%' and op_date = '%s'" % (table,area_field,area_value,kw,date))
    result = cursor.fetchall()
    conn.close()
    res_lst = []
    for each_res in result:
        res_lst.append(each_res[0])
    return res_lst

#影片票房预测
@app.route("/data/prdt_film/api")
def predict_film_api():
    return_dict = {"code":0,"msg":"处理成功","result":False}
    if request.args is None:
        return_dict["return_code"] = 504
        return_dict["return_info"] = "请求参数为空"
        return json.dumps(return_dict,ensure_ascii = False,cls = encoder)
    get_data = request.args.to_dict()
    area_field = ""
    area_value = ""
    prdt_date = get_data.get("prdt_date")
    fetch_date = get_data.get("fetch_date")
    table = get_data.get("table")
    page = 1
    limit = 30
    if "page" in get_data.keys() and "limit" in get_data.keys() and "table" in get_data.keys():
        page = int(get_data.get("page"))
        limit = int(get_data.get("limit"))
        if "area_field" in get_data.keys() and "area_value" in get_data.keys():
            area_field = get_data.get("area_field")
            area_value = get_data.get("area_value")
    return_dict["result"],return_dict["total"] = sql_predict_film(prdt_date,fetch_date,table,area_field,area_value,page,limit)

    return json.dumps(return_dict,ensure_ascii = False,cls = encoder)

#注意权限分配
#影片预测票房查询
def sql_predict_film(prdt_date,fetch_date,table,area_field,area_value,page,limit):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
    cursor = conn.cursor()
    order_val = "predict_bo_percent"
    if area_value != "":
        cursor.execute("select count(*) from %s where %s = '%s' and predict_date = '%s' and fetch_date = '%s'" % (table,area_field,area_value,prdt_date,fetch_date))
        length = cursor.fetchall()[0][0]
        cursor.execute("select %s from %s where %s = '%s' and predict_date = '%s' and fetch_date = '%s' order by %s desc limit %d,%d" % (fields_dict[table],table,area_field,area_value,prdt_date,fetch_date,order_val,(page - 1) * limit,limit))

    else:
        cursor.execute("select count(*) from %s where predict_date = '%s' and fetch_date = '%s'" % (table,prdt_date,fetch_date))
        length = cursor.fetchall()[0][0]
        cursor.execute("select %s from %s where predict_date = '%s' and fetch_date = '%s' order by %s desc limit %d,%d" % (fields_dict[table],table,prdt_date,fetch_date,order_val,(page - 1) * limit,limit))

    result = cursor.fetchall()
    fields = cursor.description
    conn.close()
    res_lst = []
    fields_lst = []
    for each_field in fields:
        fields_lst.append(each_field[0])
    for each_res in result:
        each_res = user_auth_area_data_filter(each_res)
        if each_res != None:
            res_lst.append(dict(zip(fields_lst,each_res)))
    return res_lst,length
    

#走势图数据查询
def sql_chart(table,field,area_field,area_value,date):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
    sql = ""
    if area_value == "":
        sql = "select film,%s,fetch_date from %s where presale_date = '%s' order by id asc" % (field,table,date)
    else:
        if session["user_auth_role"] == "city":
            sql = "select film,%s,fetch_date from %s where %s = '%s' and city = '%s' and presale_date = '%s' order by id asc" % (field,table,area_field,area_value,session["user_auth_area"],date)
        elif session["user_auth_role"] == "cinema":
            sql = "select film,%s,fetch_date from %s where %s = '%s' and cinema = '%s' and presale_date = '%s' order by id asc" % (field,table,area_field,area_value,session["user_auth_area"],date)
    res = pd.read_sql(sql,conn)
    df = pd.DataFrame(res)
    df["fetch_date"] = df["fetch_date"].astype(str)
    film_list = df["film"].drop_duplicates().tolist()
    fetch_date_list = df["fetch_date"].drop_duplicates().tolist()
    data_list = []

    for each_film in film_list:
        film_data = []
        for each_date in fetch_date_list:
            field_val = df[df["film"].isin([each_film]) & df["fetch_date"].isin([each_date])][field].values.tolist()
            if len(field_val) == 0:
                field_val.append(0)
            film_data.append(field_val[0])
        data_list.append([each_film,film_data])

    return fetch_date_list,data_list

#场次特征检查
@app.route("/data/film_feat/api")
def film_feat_api():
    return_dict = {"code":0,"msg":"处理成功","result":False}
    if request.args is None:
        return_dict["return_code"] = 504
        return_dict["return_info"] = "请求参数为空"
        return json.dumps(return_dict,ensure_ascii = False,cls = encoder)
    get_data = request.args.to_dict()
    area_field = ""
    area_value = ""
    table = get_data.get("table")
    date = get_data.get("date")
    page = 1
    limit = 30
    if "page" in get_data.keys() and "limit" in get_data.keys() and "table" in get_data.keys():
        page = int(get_data.get("page"))
        limit = int(get_data.get("limit"))
        if "area_field" in get_data.keys() and "area_value" in get_data.keys():
            area_field = get_data.get("area_field")
            area_value = get_data.get("area_value")
    return_dict["result"],return_dict["total"] = sql_film_feature(date,table,area_field,area_value,page,limit)

    return json.dumps(return_dict,ensure_ascii = False,cls = encoder)

#场次特征检查查询
def sql_film_feature(date,table,area_field,area_value,page,limit):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
    cursor = conn.cursor()
    if area_field == "" and area_value == "":
        cursor.execute("select count(*) from %s where op_date = '%s'" % (table,date))
        length = cursor.fetchall()[0][0]
        cursor.execute("select %s from %s where op_date = '%s' limit %d,%d" % (fields_dict[table],table,date,(page - 1) * limit,limit))
    elif area_field != "" and area_value != "":
        cursor.execute("select count(*) from %s where op_date = '%s' and %s = '%s'" % (table,date,area_field,area_value))
        length = cursor.fetchall()[0][0]
        cursor.execute("select %s from %s where op_date = '%s' and %s = '%s' limit %d,%d" % (fields_dict[table],table,date,area_field,area_value,(page - 1) * limit,limit))

    result = cursor.fetchall()
    fields = cursor.description
    conn.close()
    res_lst = []
    fields_lst = []
    for each_field in fields:
        fields_lst.append(each_field[0])
    for each_res in result:
        each_res = user_auth_area_data_filter(each_res)
        if each_res != None:
            res_lst.append(dict(zip(fields_lst,each_res)))
    return res_lst,length
    

#猫眼预售票价
@app.route("/data/my_presale/api")
def my_presale_api():
    return_dict = {"code":0,"msg":"处理成功","result":False}
    if request.args is None:
        return_dict["return_code"] = 504
        return_dict["return_info"] = "请求参数为空"
        return json.dumps(return_dict,ensure_ascii = False,cls = encoder)
    get_data = request.args.to_dict()
    area_field = ""
    area_value = ""
    show_date = get_data.get("show_date")
    begin_time = get_data.get("begin_time")
    compete_val = get_data.get("compete_relation")
    end_time = get_data.get("end_time")
    movie_name = get_data.get("movie_name")
    table = get_data.get("table")
    page = 1
    limit = 30
    if "page" in get_data.keys() and "limit" in get_data.keys() and "table" in get_data.keys():
        page = int(get_data.get("page"))
        limit = int(get_data.get("limit"))
        if "area_field" in get_data.keys() and "area_value" in get_data.keys():
            area_field = get_data.get("area_field")
            area_value = get_data.get("area_value")
    return_dict["result"],return_dict["total"] = sql_maoyan_presale(show_date,begin_time,end_time,movie_name,compete_val,table,area_field,area_value,page,limit)

    return json.dumps(return_dict,ensure_ascii = False,cls = encoder)

#注意权限分配
#猫眼预售票价查询
def sql_maoyan_presale(show_date,begin_time,end_time,movie_name,compete_val,table,area_field,area_value,page,limit):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
    cursor = conn.cursor()
    #仅限于对影城进行查询
    if area_field == "cinema":
        if movie_name == "":
            if begin_time != "" and end_time != "":
                if compete_val == "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and show_date = '%s' and show_time >= '%s' and show_time <= '%s'" % (table,area_value,show_date,begin_time,end_time))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and show_date = '%s' and show_time >= '%s' and show_time <= '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,show_date,begin_time,end_time,(page - 1)*limit,limit))
                elif compete_val != "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and show_date = '%s' and show_time >= '%s' and show_time <= '%s' and compete_relation = '%s'" % (table,area_value,show_date,begin_time,end_time,compete_val))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and show_date = '%s' and show_time >= '%s' and show_time <= '%s' and compete_relation = '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,show_date,begin_time,end_time,compete_val,(page - 1)*limit,limit))

            elif begin_time != "" and end_time == "":
                if compete_val == "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and show_date = '%s' and show_time >= '%s'" % (table,area_value,show_date,begin_time))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and show_date = '%s' and show_time >= '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,show_date,begin_time,(page - 1)*limit,limit))
                elif compete_val != "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and show_date = '%s' and show_time >= '%s' and compete_relation = '%s'" % (table,area_value,show_date,begin_time,compete_val))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and show_date = '%s' and show_time >= '%s' and compete_relation = '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,show_date,begin_time,compete_val,(page - 1)*limit,limit))

            elif begin_time == "" and end_time != "":
                if compete_val == "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and show_date = '%s' and show_time <= '%s'" % (table,area_value,show_date,end_time))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and show_date = '%s' and show_time <= '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,show_date,end_time,(page - 1)*limit,limit))
                elif compete_val != "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and show_date = '%s' and show_time <= '%s' and compete_relation = '%s'" % (table,area_value,show_date,end_time,compete_val))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and show_date = '%s' and show_time <= '%s' and compete_relation = '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,show_date,end_time,compete_val,(page - 1)*limit,limit))

            elif begin_time == "" and end_time == "":
                if compete_val == "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and show_date = '%s'" % (table,area_value,show_date))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and show_date = '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,show_date,(page - 1)*limit,limit))
                elif compete_val != "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and show_date = '%s' and compete_relation = '%s'" % (table,area_value,show_date,compete_val))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and show_date = '%s' and compete_relation = '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,show_date,compete_val,(page - 1)*limit,limit))

        elif movie_name != "":
            if begin_time != "" and end_time != "":
                if compete_val == "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time >= '%s' and show_time <= '%s'" %\
                        (table,area_value,movie_name,show_date,begin_time,end_time))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time >= '%s' and show_time <= '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,movie_name,show_date,begin_time,end_time,(page - 1)*limit,limit))
                elif compete_val != "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time >= '%s' and show_time <= '%s' and compete_relation = '%s'" %\
                        (table,area_value,movie_name,show_date,begin_time,end_time,compete_val))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time >= '%s' and show_time <= '%s' and compete_relation = '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,movie_name,show_date,begin_time,end_time,compete_val,(page - 1)*limit,limit))

            elif begin_time != "" and end_time == "":
                if compete_val == "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time >= '%s'" % \
                        (table,area_value,movie_name,show_date,begin_time))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time >= '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,movie_name,show_date,begin_time,(page - 1)*limit,limit))
                elif compete_val != "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time >= '%s' and compete_relation = '%s'" % \
                        (table,area_value,movie_name,show_date,begin_time,compete_val))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time >= '%s' and compete_relation = '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,movie_name,show_date,begin_time,compete_val,(page - 1)*limit,limit))

            elif begin_time == "" and end_time != "":
                if compete_val == "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time <= '%s'" % (table,area_value,movie_name,show_date,end_time))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time <= '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,movie_name,show_date,end_time,(page - 1)*limit,limit))
                elif compete_val != "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time <= '%s' and compete_relation = '%s'" % (table,area_value,movie_name,show_date,end_time,compete_val))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and show_time <= '%s' and compete_relation = '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,movie_name,show_date,end_time,compete_val,(page - 1)*limit,limit))

            elif begin_time == "" and end_time == "":
                if compete_val == "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s'" % (table,area_value,movie_name,show_date))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,movie_name,show_date,(page - 1)*limit,limit))
                elif compete_val != "":
                    cursor.execute("select count(*) from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and compete_relation = '%s'" % (table,area_value,movie_name,show_date,compete_val))
                    length = cursor.fetchall()[0][0]
                    cursor.execute("select jycinema_name,compete_cinema_name,compete_relation,city,film_center,movie_name,show_date,show_time,price,sold_count,hall_name,seats,version \
                        from %s where jycinema_name = '%s' and movie_name = '%s' and show_date = '%s' and compete_relation = '%s' order by show_time asc limit %d,%d" % \
                        (table,area_value,movie_name,show_date,compete_val,(page - 1)*limit,limit))


    result = cursor.fetchall()
    fields = cursor.description
    conn.close()
    res_lst = []
    fields_lst = []
    for each_field in fields:
        fields_lst.append(each_field[0])
    for each_res in result:
        each_res = user_auth_area_data_filter(each_res)
        if each_res != None:
            res_lst.append(dict(zip(fields_lst,each_res)))
    return res_lst,length


#获取更新时间
@app.context_processor
def update_time():
    time_val = UpdateTimelist.query.order_by(db.desc(UpdateTimelist.id)).first().update_time
    return {"update_time":time_val}

#数据查询过滤器
def user_auth_area_data_filter(each_data):
    if session["user_auth_area"] == "admin":
        return each_data
    else:
        if session["user_auth_area"] in each_data:
            return each_data
        else:
            return None
    