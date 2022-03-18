from flask import Blueprint,session,render_template

admin_blue = Blueprint("admin",__name__,url_prefix='/admin')

#未登录跳转处理
def login_verify(html):
    if session["auth"] == "authenrized":
        return render_template(html)
    elif session["auth"] == "":
        return redirect(url_for("login"))

@admin_blue.route("/table_list")
def table_list():
    return login_verify("admin/data_table/table_list.html")

@admin_blue.route("/table_list/total_table")
def total_table():
    return login_verify("admin/data_table/total_table.html")

@admin_blue.route("/table_list/film_center_table")
def film_center_table():
    return login_verify("admin/data_table/film_center_table.html")

@admin_blue.route("/table_list/city_table")
def city_table():
    return login_verify("admin/data_table/city_table.html")

@admin_blue.route("/table_list/cinema_table")
def cinema_table():
    return login_verify("admin/data_table/cinema_table.html")

@admin_blue.route("/table_list/session_detail_table")
def session_detail_table():
    return login_verify("admin/data_table/session_detail_table.html")

@admin_blue.route("/table_list/session_status_statistic")
def session_status_statistic():
    return login_verify("admin/data_table/session_status_statistic.html")

@admin_blue.route("/table_list/red_film_table")
def red_film_table():
    return login_verify("admin/data_table/red_film_table.html")

@admin_blue.route("/table_list/red_film_abnormal")
def red_film_abnormal():
    return login_verify("admin/data_table/red_film_abnormal.html")

@admin_blue.route("/table_list/not_open_film_cinema")
def not_open_film_cinema():
    return login_verify("admin/data_table/not_open_film_cinema.html")

@admin_blue.route("/table_list/film_feature_check")
def film_feature_check():
    return login_verify("/admin/data_table/film_feature_check.html")

@admin_blue.route("/table_list/data_table/maoyan_presale_price")
def maoyan_presale_price():
    return login_verify("admin/data_table/maoyan_presale_price.html")

#预售走势列表页面
@admin_blue.route("/chart_list")
def chart_list():
    return login_verify("admin/presale_chart/chart_list.html")

@admin_blue.route("/chart_list/total_chart")
def total_chart():
    return login_verify("admin/presale_chart/total_chart.html")

@admin_blue.route("/chart_list/film_center_chart")
def film_center_chart():
    return login_verify("admin/presale_chart/film_center_chart.html")

@admin_blue.route("/chart_list/city_chart")
def city_chart():
    return login_verify("admin/presale_chart/city_chart.html")

@admin_blue.route("/chart_list/cinema_chart")
def cinema_chart():
    return login_verify("admin/presale_chart/cinema_chart.html")

#票房预测
@admin_blue.route("/predict_list")
def predict_list():
    return login_verify("admin/predict_table/predict_list.html")

@admin_blue.route("/predict_list/predict_cinema_table")
def predict_cinema_table():
    return login_verify("admin/predict_table/predict_cinema_table.html")