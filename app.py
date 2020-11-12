import os
from flask import Flask,render_template,url_for,redirect,flash
from flask_wtf import FlaskForm
from wtforms import Form,StringField,PasswordField,BooleanField,SubmitField
from wtforms.validators import DataRequired,Length

app = Flask(__name__)
app.secret_key = os.getenv("SECRET KEY","secret string")
app.jinja_env.trim_blocks = True
app.jinja_lstrip_block = True

#如何转为数据存储？
userdata = {"admin":"admin","xieminchao":"123","liuxiaoshi":"123","chenchengying":"123"}

class LoginForm(FlaskForm):
    username = StringField("用户名",validators = [DataRequired(message = "请输入用户名")],render_kw = {"autocomplete":"off"})
    password = PasswordField("密码",validators = [DataRequired(message = "请输入密码")])
    remember = BooleanField("记住我")
    submit = SubmitField("登录")

@app.route("/",methods = ["GET","POST"])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        username = form.username.data
        password = form.password.data
        if username in userdata.keys() and password == userdata[username]:
            flash("欢迎回来，%s" % username)
            return redirect(url_for("home"))
    return render_template("login.html",form = form)

@app.route("/home")
def home():
    return render_template("home.html")

@app.errorhandler(404)
def err_404(e):
    return render_template("errors/404.html"),404

@app.errorhandler(500)
def err_500(e):
    return render_template("errors/500.html"),500