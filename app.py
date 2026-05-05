import os
import pandas as pd

from flask import Flask, render_template, request, redirect, send_file, url_for
from flask_login import (
    LoginManager, UserMixin,
    login_user, login_required,
    logout_user, current_user
)
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text

# =========================
# APP
# =========================
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret")

# =========================
# BANCO (NEON)
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL não definida")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# 🔥 estabilidade conexão Neon
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_pre_ping": True
}

db = SQLAlchemy(app)

# =========================
# LOGIN
# =========================
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

# =========================
# MODEL USUARIO
# =========================
class User(UserMixin, db.Model):
    __tablename__ = "usuarios"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(150), unique=True, nullable=False)
    senha = db.Column(db.String(200), nullable=False)

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# =========================
# 🔥 GARANTE TABELA EM PRODUÇÃO
# =========================
with app.app_context():
    db.create_all()

# =========================
# FILTROS
# =========================
@app.template_filter('numero_br')
def numero_br(valor):
    try:
        return "{:,}".format(int(valor)).replace(",", ".")
    except:
        return valor

@app.template_filter('data_br')
def data_br(valor):
    try:
        if not valor:
            return "-"
        valor = str(valor)
        return f"{valor[6:8]}/{valor[4:6]}/{valor[0:4]}"
    except:
        return valor

# =========================
# MUNICIPIOS (PROTEGIDO)
# =========================
mapa_municipios = {}

try:
    if os.path.exists("F.K03200$Z.D60411.MUNICCSV"):
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

except Exception as e:
    print("⚠️ ERRO MUNICIPIOS:", e)

# =========================
# BUSCA EMPRESAS
# =========================
def buscar_empresas(cidade, uf, palavra, data_min):

    try:
        db.session.rollback()
    except:
        pass

    query = """
    SELECT 
        cnpj,
        nome,
        uf,
        municipio,
        data_inicio,
        telefone,
        telefone2
    FROM empresas
    WHERE 1=1
    """

    params = {}

    if uf:
        query += " AND uf = :uf"
        params["uf"] = uf.upper()

    if cidade:
        query += " AND municipio = :cidade"
        params["cidade"] = cidade

    if palavra:
        query += " AND UPPER(nome) LIKE :palavra"
        params["palavra"] = f"%{palavra.upper()}%"

    if data_min:
        query += " AND data_inicio >= :data_min"
        params["data_min"] = data_min.replace("-", "")

    # 🔥 filtro de contato (adaptado pro seu banco)
    query += """
    AND (
        (telefone IS NOT NULL AND telefone != '')
        OR
        (telefone2 IS NOT NULL AND telefone2 != '')
    )
    """

    # 🔥 ESSENCIAL pra não travar com 70 milhões
    query += " ORDER BY nome LIMIT 100"

    try:
        result = db.session.execute(text(query), params)
        rows = result.fetchall()

    except Exception as e:
        print("ERRO BUSCA:", e)
        db.session.rollback()
        return []

    dados = []

    for row in rows:
        telefone = row[5] or row[6] or ""

        dados.append({
            "cnpj": row[0],
            "nome": row[1],
            "uf": row[2],
            "municipio": row[3],
            "data": row[4],
            "telefone": telefone,
            "email": "",   # não existe na sua tabela atual
            "cnae": ""     # não existe na sua tabela atual
        })

    return dados
# =========================
# HOME
# =========================
@app.route("/")
def home():
    return render_template("home.html")

# =========================
# BUSCAR
# =========================
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

            dados_formatados.append({
                "cnpj": row["cnpj"],
                "nome": row["nome"] or "Sem nome",
                "uf": row["uf"],
                "municipio": nome_municipio,
                "data": row["data"],
                "telefone": row["telefone"],
                "email": row["email"],
                "cnae": row["cnae"]
            })

        return render_template("resultados.html", dados=dados_formatados)

    return render_template("index.html", municipios=mapa_municipios)

# =========================
# DASHBOARD
# =========================
from sqlalchemy import text

@app.route("/dashboard")
@login_required
def dashboard():

    # 🔥 IMPORTANTE: limpa transação quebrada
    try:
        db.session.rollback()
    except:
        pass

    def safe_scalar(query):
        try:
            return db.session.execute(text(query)).scalar() or 0
        except Exception as e:
            print("ERRO scalar:", e)
            db.session.rollback()
            return 0

    def safe_fetchone(query):
        try:
            return db.session.execute(text(query)).fetchone()
        except Exception as e:
            print("ERRO fetchone:", e)
            db.session.rollback()
            return None

    def safe_fetchall(query):
        try:
            return db.session.execute(text(query)).fetchall()
        except Exception as e:
            print("ERRO fetchall:", e)
            db.session.rollback()
            return []

    # =========================
    # DADOS PRINCIPAIS
    # =========================

    total_empresas = safe_scalar("""
        SELECT COUNT(*) FROM empresas
    """)

    # 🔥 CORRIGIDO: telefone → nome certo da sua tabela
    total_com_telefone = safe_scalar("""
        SELECT COUNT(*) FROM empresas
        WHERE telefone IS NOT NULL AND telefone != ''
    """)

    ult_30 = safe_scalar("""
        SELECT COUNT(*) FROM empresas
        WHERE data_inicio >= TO_CHAR(NOW() - INTERVAL '30 days', 'YYYYMMDD')
    """)

    ult_60 = safe_scalar("""
        SELECT COUNT(*) FROM empresas
        WHERE data_inicio >= TO_CHAR(NOW() - INTERVAL '60 days', 'YYYYMMDD')
    """)

    ult_90 = safe_scalar("""
        SELECT COUNT(*) FROM empresas
        WHERE data_inicio >= TO_CHAR(NOW() - INTERVAL '90 days', 'YYYYMMDD')
    """)

    # =========================
    # TOP UF (seguro)
    # =========================

    try:
        top_uf = safe_fetchone("""
            SELECT uf, COUNT(*) as total
            FROM empresas
            GROUP BY uf
            ORDER BY total DESC
            LIMIT 1
        """)
    except:
        top_uf = None

    # =========================
    # MUNICIPIOS (sem cache)
    # =========================

    top_municipios_raw = safe_fetchall("""
        SELECT municipio, COUNT(*) as total
        FROM empresas
        GROUP BY municipio
        ORDER BY total DESC
        LIMIT 5
    """)

    top_municipios = [
        (mapa_municipios.get(str(cod).strip(), cod), total)
        for cod, total in top_municipios_raw
    ]

    grafico_raw = safe_fetchall("""
        SELECT municipio, COUNT(*) as total
        FROM empresas
        GROUP BY municipio
        ORDER BY total DESC
        LIMIT 10
    """)

    grafico = [
        (mapa_municipios.get(str(cod).strip(), cod), total)
        for cod, total in grafico_raw
    ]

    # =========================
    # SNAPSHOT (opcional)
    # =========================

    anterior = safe_fetchone("""
        SELECT total_empresas, total_telefone
        FROM dashboard_snapshot
        ORDER BY id DESC
        LIMIT 1 OFFSET 1
    """)

    if anterior:
        total_empresas_ant, total_tel_ant = anterior
    else:
        total_empresas_ant = total_empresas
        total_tel_ant = total_com_telefone

    def calc_percentual(atual, anterior):
        if anterior == 0:
            return 0
        return round(((atual - anterior) / anterior) * 100, 2)

    perc_empresas = calc_percentual(total_empresas, total_empresas_ant)
    perc_tel = calc_percentual(total_com_telefone, total_tel_ant)

    # =========================
    # DATA ATUALIZAÇÃO (seguro)
    # =========================

    row = safe_fetchone("""
        SELECT ultima_atualizacao
        FROM sistema_info
        ORDER BY id DESC
        LIMIT 1
    """)

    data_atualizacao = row[0] if row else "Não definido"

    # =========================
    # RENDER
    # =========================

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
        total_empresas_ant=total_empresas_ant,
        total_tel_ant=total_tel_ant,
        perc_empresas=perc_empresas,
        perc_tel=perc_tel,
        data_atualizacao=data_atualizacao
    )
# =========================
# LOGIN
# =========================
@app.route("/login", methods=["GET", "POST"])
def login():
    erro = None

    if request.method == "POST":
        email = request.form.get("email")
        senha = request.form.get("password")

        user = User.query.filter_by(email=email).first()

        if user and check_password_hash(user.senha, senha):
            login_user(user)
            next_page = request.args.get("next")
            return redirect(next_page or "/dashboard")
        else:
            erro = "Email ou senha inválidos"

    return render_template("login.html", erro=erro)

# =========================
# REGISTER
# =========================
@app.route("/register", methods=["GET", "POST"])
def register():
    sucesso = False
    erro = None

    if request.method == "POST":
        email = request.form.get("email")
        senha = request.form.get("password")

        try:
            if not email or not senha:
                erro = "Preencha todos os campos"

            elif User.query.filter_by(email=email).first():
                erro = "Email já cadastrado"

            else:
                novo = User(
                    email=email,
                    senha=generate_password_hash(senha)
                )

                db.session.add(novo)
                db.session.commit()

                sucesso = True

        except Exception as e:
            db.session.rollback()
            erro = str(e)

    return render_template("register.html", sucesso=sucesso, erro=erro)

# =========================
# LOGOUT
# =========================
@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect("/login")

# =========================
# DOWNLOAD
# =========================
@app.route("/download")
@login_required
def download():
    if not os.path.exists("resultado.csv"):
        return "Arquivo não encontrado", 404

    return send_file("resultado.csv", as_attachment=True)

# =========================
# RUN LOCAL
# =========================
if __name__ == "__main__":
    app.run(debug=True)