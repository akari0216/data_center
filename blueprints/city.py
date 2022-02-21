from flask import Blueprint,session,render_template

city_blue = Blueprint("city",__name__,url_prefix="/city")

#未登录跳转处理
def login_verify(html):
    if session["auth"] == "authenrized":
        return render_template(html)
    elif session["auth"] == "":
        return redirect(url_for("login"))

@city_blue.route("/table_list")
def table_list():
    return login_verify("city/data_table/table_list.html")

@city_blue.route("/table_list/city_table")
def city_table():
    return login_verify("city/data_table/city_table.html")

@city_blue.route("/table_list/cinema_table")
def cinema_table():
    return login_verify("city/data_table/cinema_table.html")

@city_blue.route("/table_list/session_detail_table")
def session_detail_table():
    return login_verify("city/data_table/session_detail_table.html")

@city_blue.route("/table_list/session_status_statistic")
def session_status_statistic():
    return login_verify("city/data_table/session_status_statistic.html")

@city_blue.route("/table_list/red_film_table")
def red_film_table():
    return login_verify("city/data_table/red_film_table.html")

@city_blue.route("/table_list/red_film_abnormal")
def red_film_abnormal():
    return login_verify("city/data_table/red_film_abnormal.html")

@city_blue.route("/table_list/not_open_film_cinema.html")
def not_open_film_cinema():
    return login_verify("city/data_table/not_open_film_cinema.html")

@city_blue.route("/table_list/data_table/maoyan_presale_price")
def maoyan_presale_price():
    return login_verify("city/data_table/maoyan_presale_price.html")

#预售走势列表页面
@city_blue.route("/chart_list")
def chart_list():
    return login_verify("city/presale_chart/chart_list.html")

@city_blue.route("/chart_list/city_chart")
def city_chart():
    return login_verify("city/presale_chart/city_chart.html")

@city_blue.route("/chart_list/cinema_chart")
def cinema_chart():
    return login_verify("city/presale_chart/cinema_chart.html")

#票房预测
@city_blue.route("/predict_list")
def predict_list():
    return login_verify("city/predict_table/predict_list.html")

@city_blue.route("/predict_list/predict_cinema_table")
def predict_cinema_table():
    return login_verify("city/predict_table/predict_cinema_table.html")