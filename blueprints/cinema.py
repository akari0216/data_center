from flask import Blueprint,session,render_template

cinema_blue = Blueprint("cinema",__name__,url_prefix='/cinema')

#未登录跳转处理
def login_verify(html):
    if session["auth"] == "authenrized":
        return render_template(html)
    elif session["auth"] == "":
        return redirect(url_for("login"))

@cinema_blue.route("/table_list")
def table_list():
    return login_verify("cinema/data_table/table_list.html")

@cinema_blue.route("/table_list/cinema_table")
def cinema_table():
    return login_verify("cinema/data_table/cinema_table.html")

@cinema_blue.route("/table_list/session_detail_table")
def session_detail_table():
    return login_verify("cinema/data_table/session_detail_table.html")

@cinema_blue.route("/table_list/session_status_statistic")
def session_status_statistic():
    return login_verify("cinema/data_table/session_status_statistic.html")

@cinema_blue.route("/table_list/not_open_film_cinema.html")
def not_open_film_cinema():
    return login_verify("cinema/data_table/not_open_film_cinema.html")

#预售走势列表页面
@cinema_blue.route("/chart_list")
def chart_list():
    return login_verify("cinema/presale_chart/chart_list.html")

@cinema_blue.route("/chart_list/cinema_chart")
def cinema_chart():
    return login_verify("cinema/presale_chart/cinema_chart.html")

#票房预测
@cinema_blue.route("/predict_list")
def predict_list():
    return login_verify("cinema/predict_table/predict_list.html")

@cinema_blue.route("/predict_list/predict_cinema_table")
def predict_cinema_table():
    return login_verify("cinema/predict_table/predict_cinema_table.html")