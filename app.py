import os
import sqlite3
import pandas as pd
from flask import Flask, render_template, request, redirect, send_file
from flask_login import (
    LoginManager, UserMixin,
    login_user, login_required,
    logout_user, current_user
)
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin
import os

app = Flask(__name__)
app.secret_key = "segredo-super"

# =========================
# BANCO DE DADOS
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL não definida no Render")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

# =========================
# LOGIN MANAGER
# =========================
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

# =========================
# MODEL DE USUÁRIO (ESSENCIAL)
# =========================
class User(UserMixin, db.Model):
    __tablename__ = "usuarios"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(150), unique=True, nullable=False)
    senha = db.Column(db.String(200), nullable=False)

# =========================
# LOAD USER (FLASK LOGIN)
# =========================
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# =========================
# CRIAR TABELAS
# =========================
with app.app_context():
    db.create_all()
# 🔢 filtro número BR
@app.template_filter('numero_br')
def numero_br(valor):
    try:
        return "{:,}".format(int(valor)).replace(",", ".")
    except:
        return valor

# 📅 filtro data BR
@app.template_filter('data_br')
def data_br(valor):
    try:
        if not valor:
            return "-"
        valor = str(valor)
        return f"{valor[6:8]}/{valor[4:6]}/{valor[0:4]}"
    except:
        return valor


# 🔹 carregar municípios
municipios_df = pd.read_csv(
    "F.K03200$Z.D60411.MUNICCSV",
    sep=";",
    dtype=str,
    encoding="latin1"
)

municipios_df.columns = ["codigo", "nome"]

mapa_municipios = dict(zip(
    municipios_df["codigo"].str.strip(),
    municipios_df["nome"].str.strip().str.upper()
))


def buscar_empresas(cidade, uf, palavra, data_min):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    query = """
    SELECT 
        CNPJ,
        NOME_FANTASIA,
        UF,
        MUNICIPIO,
        DATA_INICIO_ATIVIDADE,
        DDD_1,
        TELEFONE_1,
        CORREIO_ELETRONICO,
        CNAE_FISCAL_PRINCIPAL
    FROM empresas
    WHERE 1=1
    """

    params = []

    if uf:
        query += " AND UF = ?"
        params.append(uf.upper())

    if cidade:
        query += " AND MUNICIPIO = ?"
        params.append(cidade)

    if palavra:
        query += " AND UPPER(NOME_FANTASIA) LIKE ?"
        params.append(f"%{palavra.upper()}%")

    if data_min:
        query += " AND DATA_INICIO_ATIVIDADE >= ?"
        params.append(data_min.replace("-", ""))

    query += """
    AND (
        (TELEFONE_1 IS NOT NULL AND TELEFONE_1 != '')
        OR
        (CORREIO_ELETRONICO IS NOT NULL AND CORREIO_ELETRONICO != '')
    )
    """

    query += " LIMIT 200"

    cursor.execute(query, params)
    rows = cursor.fetchall()

    conn.close()

    # 🔥 CONVERTE PARA DICIONÁRIO (ESSENCIAL)
    dados = []

    for row in rows:
        ddd = row[5] or ""
        tel = row[6] or ""

        telefone = ""

        if ddd and tel:
            telefone = f"{ddd}{tel}"
        elif tel:
            telefone = tel

        dados.append({
            "cnpj": row[0],
            "nome": row[1],
            "uf": row[2],
            "municipio": row[3],
            "data": row[4],
            "telefone": telefone,
            "email": row[7],
            "cnae": row[8]
        })

    return dados

# =========================
# 🏠 HOME (NOVA)
# =========================
@app.route("/")
def home():
    return render_template("home.html")


# 🏠 HOME (PROTEGIDA)
@app.route("/buscar", methods=["GET", "POST"])
@login_required
def index():
    if request.method == "POST":
        cidade = request.form.get("cidade")
        uf = request.form.get("uf")
        palavra = request.form.get("palavra")
        data = request.form.get("data")

        dados = buscar_empresas(cidade, uf, palavra, data)

        dados_formatados = []

        for row in dados:
            municipio_codigo = str(row["municipio"]).strip()
            nome_municipio = mapa_municipios.get(municipio_codigo, municipio_codigo)

            telefone = row["telefone"] or ""

            dados_formatados.append({
                "cnpj": row["cnpj"],
                "nome": row["nome"] or "Sem nome",
                "uf": row["uf"],
                "municipio": nome_municipio,
                "data": row["data"],
                "telefone": telefone,
                "email": row["email"],
                "cnae": row["cnae"]
            })

        return render_template("resultados.html", dados=dados_formatados)

    return render_template("index.html", municipios=mapa_municipios)

# 📊 DASHBOARD
from datetime import datetime, timedelta

@app.route("/dashboard")
@login_required
def dashboard():
    conn = sqlite3.connect("empresas.db")
    cursor = conn.cursor()

    # TOTAL EMPRESAS
    cursor.execute("SELECT COUNT(*) FROM empresas")
    total_empresas = cursor.fetchone()[0]

    # COM TELEFONE
    cursor.execute("""
        SELECT COUNT(*) FROM empresas
        WHERE TELEFONE_1 IS NOT NULL AND TELEFONE_1 != ''
    """)
    total_com_telefone = cursor.fetchone()[0]

    # ÚLTIMOS 30 DIAS
    cursor.execute("""
        SELECT COUNT(*) FROM empresas
        WHERE DATA_INICIO_ATIVIDADE >= strftime('%Y%m%d','now','-30 day')
    """)
    ult_30 = cursor.fetchone()[0]

    # ÚLTIMOS 60 DIAS
    cursor.execute("""
        SELECT COUNT(*) FROM empresas
        WHERE DATA_INICIO_ATIVIDADE >= strftime('%Y%m%d','now','-60 day')
    """)
    ult_60 = cursor.fetchone()[0]

    # ÚLTIMOS 90 DIAS
    cursor.execute("""
        SELECT COUNT(*) FROM empresas
        WHERE DATA_INICIO_ATIVIDADE >= strftime('%Y%m%d','now','-90 day')
    """)
    ult_90 = cursor.fetchone()[0]

    # TOP UF (CACHE)
    cursor.execute("""
        SELECT UF, total
        FROM cache_uf
        ORDER BY total DESC
        LIMIT 1
    """)
    top_uf = cursor.fetchone()

    # TOP MUNICÍPIOS (CACHE)
    cursor.execute("""
        SELECT MUNICIPIO, total
        FROM cache_municipios
        ORDER BY total DESC
        LIMIT 5
    """)
    top_municipios_raw = cursor.fetchall()

    top_municipios = []
    for cod, total in top_municipios_raw:
        nome = mapa_municipios.get(str(cod).strip(), cod)
        top_municipios.append((nome, total))

    # GRÁFICO (CACHE)
    cursor.execute("""
        SELECT MUNICIPIO, total
        FROM cache_municipios
        ORDER BY total DESC
        LIMIT 10
    """)
    dados_brutos = cursor.fetchall()

    grafico = []
    for cod, total in dados_brutos:
        nome = mapa_municipios.get(str(cod).strip(), cod)
        grafico.append((nome, total))

    # 🔥 HISTÓRICO REAL
    cursor.execute("""
        SELECT total_empresas, total_telefone
        FROM dashboard_snapshot
        ORDER BY id DESC
        LIMIT 1 OFFSET 1
    """)
    anterior = cursor.fetchone()

    if anterior:
        total_empresas_ant, total_tel_ant = anterior
    else:
        total_empresas_ant = total_empresas
        total_tel_ant = total_com_telefone

    # 🔥 CÁLCULO %
    def calc_percentual(atual, anterior):
        if anterior == 0:
            return 0
        return round(((atual - anterior) / anterior) * 100, 2)

    perc_empresas = calc_percentual(total_empresas, total_empresas_ant)
    perc_tel = calc_percentual(total_com_telefone, total_tel_ant)

    # 🔥 DATA REAL DA BASE (CORRETO)
    cursor.execute("""
        SELECT ultima_atualizacao
        FROM sistema_info
        ORDER BY id DESC
        LIMIT 1
    """)

    row = cursor.fetchone()

    if row:
        data_atualizacao = row[0]
    else:
        data_atualizacao = "Não definido"

    conn.close()

    return render_template(
        "dashboard.html",
        total_empresas=total_empresas,
        total_com_telefone=total_com_telefone,
        ult_30=ult_30,
        ult_60=ult_60,
        ult_90=ult_90,
        top_uf=top_uf,
        top_municipios=top_municipios,
        grafico=grafico,

        # NOVOS
        total_empresas_ant=total_empresas_ant,
        total_tel_ant=total_tel_ant,
        perc_empresas=perc_empresas,
        perc_tel=perc_tel,
        data_atualizacao=data_atualizacao
    )
    
# 🔐 LOGIN
@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect("/dashboard")

    erro = None

    if request.method == "POST":
        email = request.form.get("email")
        senha = request.form.get("password")

        user = User.query.filter_by(email=email).first()

        if user and check_password_hash(user.senha, senha):
            login_user(user)
            return redirect("/dashboard")
        else:
            erro = "Email ou senha incorretos"

    return render_template("login.html", erro=erro)

# 📝 REGISTER
from flask import flash, redirect, url_for

@app.route("/register", methods=["GET", "POST"])
def register():
    sucesso = False
    erro = None

    if request.method == "POST":
        email = request.form.get("email")
        senha = request.form.get("senha")

        if User.query.filter_by(email=email).first():
            erro = "⚠️ Este email já está cadastrado."
        else:
            novo_user = User(
                email=email,
                senha=generate_password_hash(senha)
            )

            db.session.add(novo_user)
            db.session.commit()

            sucesso = True

    return render_template("register.html", sucesso=sucesso, erro=erro)


# 🚪 LOGOUT
@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect("/login")


# 📥 DOWNLOAD
@app.route("/download")
@login_required
def download():
    return send_file("resultado.csv", as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)