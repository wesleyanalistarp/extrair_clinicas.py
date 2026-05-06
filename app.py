import os
import pandas as pd

from flask import Flask, render_template, request, jsonify, redirect, send_file, url_for
from flask_login import (
    LoginManager, UserMixin,
    login_user, login_required,
    logout_user, current_user
)
from flask import send_from_directory
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text
from datetime import datetime
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

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


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, 'static'),
        'favicon.ico',
        mimetype='image/vnd.microsoft.icon'
    )
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
from sqlalchemy import text

def buscar_empresas(cidade, uf, palavra, data_min, status):

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
        telefone2,
        status,
        observacao
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

    # 🔥 FILTRO DE STATUS
    if status:
        if status == "novo":
            query += " AND (status IS NULL OR status = '')"
        else:
            query += " AND status = :status"
            params["status"] = status

    # 🔥 FILTRO DE CONTATO
    query += """
    AND (
        (telefone IS NOT NULL AND telefone != '')
        OR
        (telefone2 IS NOT NULL AND telefone2 != '')
    )
    """

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

        # 📅 FORMATA DATA (YYYYMMDD → DD/MM/YYYY)
        data_raw = str(row[4]) if row[4] else ""
        data_formatada = ""

        if len(data_raw) == 8:
            data_formatada = f"{data_raw[6:8]}/{data_raw[4:6]}/{data_raw[0:4]}"

        # 📞 FORMATA TELEFONE
        tel = str(row[5] or row[6] or "").replace(" ", "").replace("-", "")

        telefone_formatado = ""
        if len(tel) == 11:
            telefone_formatado = f"({tel[:2]}) {tel[2:7]}-{tel[7:]}"
        elif len(tel) == 10:
            telefone_formatado = f"({tel[:2]}) {tel[2:6]}-{tel[6:]}"
        else:
            telefone_formatado = tel

        dados.append({
            "cnpj": row[0],
            "nome": row[1],
            "uf": row[2],
            "municipio": row[3],
            "data": data_formatada,
            "telefone": telefone_formatado,
            "status": row[7] or "",
            "observacao": row[8] or "",
            "email": "",
            "cnae": ""
        })

    return dados

    #===============
    #ROTA ATUALIZAR STATUS
@app.route("/atualizar_status", methods=["POST"])
@login_required
def atualizar_status():

    try:
        db.session.rollback()
    except:
        pass

    cnpj = request.json.get("cnpj")
    status = request.json.get("status")
    observacao = request.json.get("observacao", "")

    try:
        db.session.execute(text("""
            UPDATE empresas
            SET status = :status,
                ultima_acao = :data,
                observacao = :obs
            WHERE cnpj = :cnpj
        """), {
            "status": status,
            "data": datetime.now(),
            "obs": observacao,
            "cnpj": cnpj
        })

        db.session.commit()
        return jsonify({"success": True})

    except Exception as e:
        print("ERRO STATUS:", e)
        db.session.rollback()
        return jsonify({"success": False})
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
        status = request.form.get("status")

        dados = buscar_empresas(cidade, uf, palavra, data, status)

        dados_formatados = []

        for row in dados:
            municipio_codigo = str(row["municipio"]).strip()
            nome_municipio = mapa_municipios.get(municipio_codigo, municipio_codigo)

            dados_formatados.append({
                "cnpj": row["cnpj"],
                "nome": row["nome"] or "Sem nome",
                "uf": row["uf"],
                "municipio": nome_municipio,
                "data": row["data"],              # ✅ já vem formatado
                "telefone": row["telefone"],      # ✅ já vem formatado
                "status": row["status"],
                "observacao": row["observacao"]
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

    def safe_scalar(query):
        try:
            return db.session.execute(text(query)).scalar() or 0
        except:
            db.session.rollback()
            return 0

    def safe_fetchone(query):
        try:
            return db.session.execute(text(query)).fetchone()
        except:
            db.session.rollback()
            return None

    def safe_fetchall(query):
        try:
            return db.session.execute(text(query)).fetchall()
        except:
            db.session.rollback()
            return []

    # =========================
    # KPIs
    # =========================

    total_empresas = safe_scalar("SELECT COUNT(*) FROM empresas")

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
    # TOP UF
    # =========================

    top_uf = safe_fetchone("""
        SELECT uf, COUNT(*) as total
        FROM empresas
        GROUP BY uf
        ORDER BY total DESC
        LIMIT 1
    """)

    # =========================
    # TOP MUNICÍPIOS
    # =========================

    top_municipios = safe_fetchall("""
        SELECT municipio, COUNT(*) as total
        FROM empresas
        GROUP BY municipio
        ORDER BY total DESC
        LIMIT 5
    """)

    # =========================
    # SNAPSHOT (comparação)
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
    # DATA ATUALIZAÇÃO
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
    grafico_raw = safe_fetchall("""
    SELECT municipio, COUNT(*) as total
    FROM empresas
    GROUP BY municipio
    ORDER BY total DESC
    LIMIT 10
    """)

    grafico_raw = safe_fetchall("""
    SELECT municipio, COUNT(*) as total
    FROM empresas
    GROUP BY municipio
    ORDER BY total DESC
    LIMIT 10
    """)

    grafico = [
    (mapa_municipios.get(str(cod).strip(), str(cod)), int(total))
    for cod, total in grafico_raw
    ]

    # 👇 MESMO NÍVEL (sem espaço extra)
    return render_template(
        "dashboard.html",
        total_empresas=total_empresas,
        total_com_telefone=total_com_telefone,
        ult_30=ult_30,
        ult_60=ult_60,
        ult_90=ult_90,
        top_uf=top_uf,
        top_municipios=top_municipios,
        total_empresas_ant=total_empresas_ant,
        total_tel_ant=total_tel_ant,
        perc_empresas=perc_empresas,
        perc_tel=perc_tel,
        data_atualizacao=data_atualizacao,
        grafico=grafico
    )
    # =========================
    # DADOS PRINCIPAIS
    # =========================
    
        # =========================
    # api dasboard
@app.route("/api/dashboard")
@login_required
def dashboard_api():
    try:
        db.session.rollback()
    except:
        pass

    # 📊 TOP CIDADES
    top_cidades = db.session.execute(text("""
        SELECT municipio, COUNT(*) as total
        FROM empresas
        GROUP BY municipio
        ORDER BY total DESC
        LIMIT 10
    """)).fetchall()

    # 📈 EVOLUÇÃO (por mês) - FORMATADA
    evolucao = db.session.execute(text("""
        SELECT SUBSTRING(data_inicio,1,6) AS mes, COUNT(*) 
        FROM empresas
        GROUP BY mes
        ORDER BY mes
        LIMIT 12
    """)).fetchall()

    # 📞 QUALIDADE LEADS
    qualidade = db.session.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE telefone IS NOT NULL AND telefone != '') AS com_tel,
            COUNT(*) FILTER (WHERE telefone IS NULL OR telefone = '') AS sem_tel
        FROM empresas
    """)).fetchone()

    # 🎯 STATUS
    status = db.session.execute(text("""
        SELECT status, COUNT(*) 
        FROM empresas
        GROUP BY status
    """)).fetchall()

    return jsonify({

        # 🔥 CIDADES COM NOME (corrigido)
        "cidades": [
            mapa_municipios.get(str(x[0]).strip(), str(x[0]))
            for x in top_cidades
        ],
        "cidades_total": [int(x[1]) for x in top_cidades],

        # 📅 EVOLUÇÃO FORMATADA (MM/AAAA)
        "meses": [
            f"{str(x[0])[4:6]}/{str(x[0])[0:4]}"
            for x in evolucao
        ],
        "meses_total": [int(x[1]) for x in evolucao],

        # 📞 QUALIDADE
        "qualidade": {
            "com_tel": int(qualidade[0] or 0),
            "sem_tel": int(qualidade[1] or 0)
        },

        # 🎯 STATUS
        "status": {
            str(x[0] or "novo"): int(x[1]) for x in status
        }
    })
# =========================
# dashboard_operacional

@app.route("/api/dashboard_operacional")
@login_required
def dashboard_operacional():
    try:
        db.session.rollback()
    except:
        pass

    # 📈 PRODUTIVIDADE (últimos 7 dias)
    produtividade = db.session.execute(text("""
        SELECT DATE(ultima_acao) as dia, COUNT(*)
        FROM empresas
        WHERE ultima_acao IS NOT NULL
        GROUP BY dia
        ORDER BY dia DESC
        LIMIT 7
    """)).fetchall()

    produtividade = list(reversed(produtividade))

    # 🎯 CONVERSÃO
    conv = db.session.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE status = 'fechado') as fechados,
            COUNT(*) FILTER (WHERE status = 'ligado') as ligados
        FROM empresas
    """)).fetchone()

    fechados = int(conv[0] or 0)
    ligados = int(conv[1] or 0)

    taxa = round((fechados / ligados) * 100, 2) if ligados > 0 else 0

    # 🔥 TOP CIDADES (FECHAMENTO)
    top_fechados = db.session.execute(text("""
        SELECT municipio, COUNT(*)
        FROM empresas
        WHERE status = 'fechado'
        GROUP BY municipio
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)).fetchall()

    return jsonify({
        "prod_dias": [str(x[0]) for x in produtividade],
        "prod_total": [int(x[1]) for x in produtividade],

        "taxa_conversao": taxa,

        "cidades_fechado": [
            mapa_municipios.get(str(x[0]).strip(), str(x[0]))
            for x in top_fechados
        ],
        "cidades_total": [int(x[1]) for x in top_fechados]
    })
# =========================
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