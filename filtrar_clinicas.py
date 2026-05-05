import psycopg2
import sqlite3

# =========================
# CONFIG
# =========================

SQLITE_DB = "empresas.db"

POSTGRES_URL = "postgresql://neondb_owner:npg_KR5TGagzE1mZ@ep-steep-dust-am57tgsu.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require"

BATCH_SIZE = 10000

# =========================
# CONEXÕES
# =========================

print("🔌 Conectando SQLite...")
sqlite_conn = sqlite3.connect(SQLITE_DB)
sqlite_cursor = sqlite_conn.cursor()

print("🔌 Conectando PostgreSQL...")
pg_conn = psycopg2.connect(POSTGRES_URL)
pg_cursor = pg_conn.cursor()

# =========================
# QUERY SQLITE (COLUNAS REAIS)
# =========================

sqlite_cursor.execute("""
SELECT 
    CNPJ,
    NOME_FANTASIA,
    UF,
    MUNICIPIO,
    DATA_INICIO_ATIVIDADE,
    DDD_1,
    TELEFONE_1
FROM empresas
WHERE UF IN ('SP','BA','MG','PE')
""")

# =========================
# FILTRO CLÍNICA
# =========================

def eh_clinica(nome):
    if not nome:
        return False

    nome = nome.lower()

    palavras = [
        "clinica", "hospital", "medico", "odont", "saude"
    ]

    return any(p in nome for p in palavras)

# =========================
# PROCESSAMENTO EM LOTES
# =========================

total = 0

while True:
    rows = sqlite_cursor.fetchmany(BATCH_SIZE)

    if not rows:
        break

    dados_filtrados = []

    for row in rows:
        cnpj, nome, uf, municipio, data, ddd, tel = row

        # filtro clínica
        if not eh_clinica(nome):
            continue

        # monta telefone
        telefone = ""
        telefone2 = ""

        if ddd and tel:
            telefone = f"{ddd}{tel}"
        elif tel:
            telefone = tel

        # filtro: precisa ter telefone
        if not telefone:
            continue

        dados_filtrados.append((
            cnpj,
            nome,
            uf,
            municipio,
            data,
            telefone,
            telefone2
        ))

    if dados_filtrados:
        args_str = ",".join(
            pg_cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s)", x).decode("utf-8")
            for x in dados_filtrados
        )

        pg_cursor.execute("""
            INSERT INTO empresas 
            (cnpj, nome, uf, municipio, data_inicio, telefone, telefone2)
            VALUES """ + args_str)

        pg_conn.commit()

        total += len(dados_filtrados)
        print(f"✅ Inseridos: {total}")

# =========================
# FINALIZAÇÃO
# =========================

pg_cursor.close()
pg_conn.close()
sqlite_conn.close()

print("🔥 FINALIZADO COM SUCESSO")