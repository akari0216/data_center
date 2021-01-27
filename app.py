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

app = Flask(__name__,static_folder = "",static_url_path = "")
app.secret_key = os.getenv("SECRET KEY","secret string")
app.jinja_env.trim_blocks = True
app.jinja_lstrip_block = True

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
            "film_session_detail":"cinema,city,film_center,hall,film,session_time,bo,people,avg_price,seats,occupancy,session_status,op_date"}

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
            print("account",account,"passwd",passwd)
            session["auth"] = "authenrized"
            session["usermsg"] = usermsg
            # 以session取代账号密码
            data.pop("account")
            data.pop("password")
            return redirect(url_for("table_list"))
        else:
            wrong_flag = 1

    return render_template("login.html",wrong_flag = wrong_flag)

#主页
@app.route("/home")
def home():
    return render_template("home.html")

#未登录跳转处理
def login_verify(html):
    if session["auth"] == "authenrized":
        return render_template(html)
    elif session["auth"] == "":
        return redirect(url_for("login"))

#数据查询列表页面
@app.route("/table_list")
def table_list():
    return login_verify("data_table/table_list.html")

@app.route("/table_list/total_table")
def total_table():
    return login_verify("data_table/total_table.html")

@app.route("/table_list/film_center_table")
def film_center_table():
    return login_verify("data_table/film_center_table.html")

@app.route("/table_list/city_table")
def city_table():
    return login_verify("data_table/city_table.html")

@app.route("/table_list/cinema_table")
def cinema_table():
    return login_verify("data_table/cinema_table.html")

@app.route("/table_list/sessoin_detail_table")
def session_detail_table():
    return login_verify("data_table/session_detail_table.html")

#报表数据接口
@app.route("/data/api")
def api():
    return_dict = {"code":0,"msg":"处理成功","result":False,"total":0}
    if request.args is None:
        return_dict["return_code"] = 504
        return_dict["return_info"] = "请求参数为空"
        return json.dumps(return_dict,ensure_ascii = False,cls = encoder)
    get_data = request.args.to_dict()
    date = get_data.get("date")
    table = get_data.get("table")
    area_field = ""
    area_value = ""
    page = 1
    limit = 30
    if "page" in get_data.keys() and "limit" in get_data.keys() and "table" in get_data.keys():
        page = int(get_data.get("page"))
        limit = int(get_data.get("limit"))
        if "area_field" in get_data.keys() and "area_value" in get_data.keys():
            area_field = get_data.get("area_field")
            area_value = get_data.get("area_value")
    return_dict["result"],return_dict["total"] = sql_result(table,area_field,area_value,date,page,limit)

    return json.dumps(return_dict,ensure_ascii = False,cls = encoder)

def sql_result(table,area_field,area_value,date,page,limit):
    conn = pymysql.connect(host = "localhost",port = 3306,user = "root",passwd = "jy123456",db = "film_data",charset = "utf8")
    cursor = conn.cursor()
    length = 0
    if area_value != "":
        cursor.execute("select count(*) from %s where op_date = '%s' and %s = '%s'" % (table,date,area_field,area_value))
        length = cursor.fetchall()[0][0]
        cursor.execute("select %s from %s where op_date = '%s'  and %s = '%s'  order by session desc limit %d,%d" % (fields_dict[table],table,date,area_field,area_value,(page - 1) * limit,limit))
    else:
        cursor.execute("select count(*) from %s where op_date = '%s'" % (table,date))
        length = cursor.fetchall()[0][0]
        cursor.execute("select %s from %s where op_date = '%s' order by session desc limit %d,%d" % (fields_dict[table],table,date,(page - 1) * limit,limit))
    result = cursor.fetchall()
    fields = cursor.description
    conn.close()
    res_lst = []
    fields_lst = []
    for each_field in fields:
        fields_lst.append(each_field[0])
    for each_res in result:
        res_lst.append(dict(zip(fields_lst,each_res)))
    return res_lst,length
    

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
