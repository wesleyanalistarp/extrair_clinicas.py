import sqlite3
import os
import csv

PASTA = "dados_receita"

conn = sqlite3.connect("empresas.db")
cursor = conn.cursor()

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

conn.commit()


def limpar(valor):
    return valor.strip() if valor else ""


def processar_arquivo(caminho):
    with open(caminho, encoding="latin1", newline="") as f:
        reader = csv.reader(f, delimiter=';', quotechar='"')

        batch = []

        for partes in reader:

            # 🔥 garante estrutura correta
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

            except Exception as e:
                continue

        if batch:
            cursor.executemany("""
                INSERT INTO empresas VALUES (?,?,?,?,?,?,?,?,?)
            """, batch)
            conn.commit()


for root, dirs, files in os.walk(PASTA):
    for file in files:
        caminho = os.path.join(root, file)

        print("🚀 Processando:", caminho)

        processar_arquivo(caminho)


conn.close()

print("✅ IMPORTAÇÃO FINALIZADA")