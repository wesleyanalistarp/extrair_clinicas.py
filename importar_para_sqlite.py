import sqlite3
import os
import csv
from datetime import datetime  # 🔥 NOVO

PASTA = "dados_receita"

conn = sqlite3.connect("empresas.db")
cursor = conn.cursor()

# 🔥 recria tabela principal
cursor.execute("DROP TABLE IF EXISTS empresas")

cursor.execute("""
CREATE TABLE empresas (
    CNPJ TEXT,
    NOME_FANTASIA TEXT,
    UF TEXT,
    MUNICIPIO TEXT,
    DATA_INICIO_ATIVIDADE TEXT,
    CNAE_FISCAL_PRINCIPAL TEXT,
    DDD_1 TEXT,
    TELEFONE_1 TEXT,
    CORREIO_ELETRONICO TEXT
)
""")

# 🔥 cria tabela de controle (NOVA)
cursor.execute("""
CREATE TABLE IF NOT EXISTS sistema_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ultima_atualizacao TEXT
)
""")

conn.commit()


def limpar(valor):
    return valor.strip() if valor else ""


def processar_arquivo(caminho):
    with open(caminho, encoding="latin1", newline="") as f:
        reader = csv.reader(f, delimiter=';', quotechar='"')

        batch = []

        for partes in reader:

            if len(partes) < 27:
                continue

            try:
                registro = (
                    limpar(partes[0] + partes[1] + partes[2]),  # CNPJ
                    limpar(partes[4]),
                    limpar(partes[19]),  # UF
                    limpar(partes[20]),  # MUNICIPIO
                    limpar(partes[10]),
                    limpar(partes[11]),
                    limpar(partes[21]),
                    limpar(partes[22]),
                    limpar(partes[26])
                )

                batch.append(registro)

                if len(batch) >= 10000:
                    cursor.executemany("""
                        INSERT INTO empresas VALUES (?,?,?,?,?,?,?,?,?)
                    """, batch)
                    conn.commit()
                    batch.clear()

            except Exception:
                continue

        if batch:
            cursor.executemany("""
                INSERT INTO empresas VALUES (?,?,?,?,?,?,?,?,?)
            """, batch)
            conn.commit()


# 🔥 IMPORTAÇÃO
for root, dirs, files in os.walk(PASTA):
    for file in files:
        caminho = os.path.join(root, file)

        print("🚀 Processando:", caminho)

        processar_arquivo(caminho)


# 🔥 SALVA DATA REAL DA BASE (ESSENCIAL)
data_importacao = datetime.now().strftime("%d/%m/%Y %H:%M")

cursor.execute("DELETE FROM sistema_info")

cursor.execute("""
INSERT INTO sistema_info (ultima_atualizacao)
VALUES (?)
""", (data_importacao,))

conn.commit()
conn.close()

print("✅ IMPORTAÇÃO FINALIZADA")
print(f"📅 Base atualizada em: {data_importacao}")