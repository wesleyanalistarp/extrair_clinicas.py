import psycopg2
import sqlite3
import csv
import io

# ==============================
# CONFIGURAÇÃO
# ==============================

SQLITE_DB = "empresas.db"

POSTGRES_URL = "postgresql://neondb_owner:npg_KR5TGagzE1mZ@ep-steep-dust-am57tgsu.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require"

BATCH_SIZE = 50000

# ==============================
# CONEXÕES
# ==============================

print("🔌 Conectando SQLite...")
sqlite_conn = sqlite3.connect(SQLITE_DB)
sqlite_cursor = sqlite_conn.cursor()

print("🔌 Conectando PostgreSQL...")
pg_conn = psycopg2.connect(POSTGRES_URL)
pg_cursor = pg_conn.cursor()

# ==============================
# QUERY SQLITE
# ==============================

query = """
SELECT 
    CNPJ as cnpj,
    NOME_FANTASIA as nome,
    UF as uf,
    MUNICIPIO as municipio,
    DATA_INICIO_ATIVIDADE as data_inicio,
    (DDD_1 || TELEFONE_1) as telefone,
    TELEFONE_1 as telefone2
FROM empresas
"""

print("📥 Lendo dados do SQLite...")
sqlite_cursor.execute(query)

# ==============================
# IMPORTAÇÃO EM LOTES
# ==============================

total = 0

while True:
    rows = sqlite_cursor.fetchmany(BATCH_SIZE)

    if not rows:
        break

    print(f"🚀 Enviando lote com {len(rows)} registros...")

    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=';', quoting=csv.QUOTE_MINIMAL)

    for row in rows:
        writer.writerow([col if col is not None else "" for col in row])

    buffer.seek(0)

    pg_cursor.copy_expert("""
        COPY empresas (cnpj, nome, uf, municipio, data_inicio, telefone, telefone2)
        FROM STDIN WITH (FORMAT CSV, DELIMITER ';')
    """, buffer)

    pg_conn.commit()

    total += len(rows)
    print(f"✅ Total importado: {total}")

# ==============================
# FINALIZAÇÃO
# ==============================

pg_cursor.close()
pg_conn.close()
sqlite_conn.close()

print("🔥 IMPORTAÇÃO FINALIZADA!")