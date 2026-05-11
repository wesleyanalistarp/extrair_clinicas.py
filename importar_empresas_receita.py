import os
import psycopg2
from psycopg2.extras import execute_batch

# =====================================
# CONFIG
# =====================================

DATABASE_URL = "postgresql://neondb_owner:npg_KR5TGagzE1mZ@ep-steep-dust-am57tgsu-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"


PASTA_DADOS = r"C:\extrair_clinicas.py\dados_receita"

BATCH_SIZE = 5000

# =====================================
# CONEXÃO
# =====================================

conn = psycopg2.connect(DATABASE_URL)

cursor = conn.cursor()

# =====================================
# BUSCAR CNPJS NECESSÁRIOS
# =====================================

print("\n🔎 Carregando CNPJs do banco...")

cursor.execute("""

    SELECT DISTINCT cnpj_base

    FROM empresas_detalhes

    WHERE (

        natureza_juridica IS NULL

        OR porte_empresa IS NULL

        OR capital_social IS NULL

        OR razao_social IS NULL

    )

    AND cnpj_base IS NOT NULL

""")

bases_necessarias = {
    row[0]
    for row in cursor.fetchall()
}

print(f"✅ {len(bases_necessarias)} bases pendentes")

# =====================================
# PROCESSAMENTO
# =====================================

updates = []

total_updates = 0

for pasta in os.listdir(PASTA_DADOS):

    if "empresa" not in pasta.lower():
        continue

    caminho_pasta = os.path.join(
        PASTA_DADOS,
        pasta
    )

    print(f"\n📂 Processando pasta: {pasta}")

    for arquivo_nome in os.listdir(caminho_pasta):

        if "EMPRECSV" not in arquivo_nome.upper():
            continue

        caminho_arquivo = os.path.join(
            caminho_pasta,
            arquivo_nome
        )

        print(f"📄 Lendo: {arquivo_nome}")

        with open(
            caminho_arquivo,
            "r",
            encoding="latin1"
        ) as arquivo:

            for linha_num, linha in enumerate(arquivo, start=1):

                partes = linha.strip().split(";")

                if len(partes) < 6:
                    continue

                cnpj_base = (
                    partes[0]
                    .replace('"', '')
                    .strip()
                    .zfill(8)
                )

                if cnpj_base not in bases_necessarias:
                    continue

                razao_social = (
                    partes[1]
                    .replace('"', '')
                    .strip()
                )

                natureza_juridica = (
                    partes[2]
                    .replace('"', '')
                    .strip()
                )

                capital_social = (
                    partes[4]
                    .replace('"', '')
                    .replace(',', '.')
                    .strip()
                )

                porte_empresa = (
                    partes[5]
                    .replace('"', '')
                    .strip()
                )

                try:

                    capital_social = float(
                        capital_social
                    )

                except:

                    capital_social = 0

                updates.append((

                    razao_social,
                    natureza_juridica,
                    porte_empresa,
                    capital_social,
                    cnpj_base

                ))

                # =====================================
                # BATCH
                # =====================================

                if len(updates) >= BATCH_SIZE:

                    execute_batch(

                        cursor,

                        """

                        UPDATE empresas_detalhes

                        SET

                            razao_social = %s,

                            natureza_juridica = %s,

                            porte_empresa = %s,

                            capital_social = %s

                        WHERE cnpj_base = %s

                        """,

                        updates

                    )

                    conn.commit()

                    total_updates += len(updates)

                    print(
                        f"✅ {total_updates} updates realizados"
                    )

                    updates.clear()

                # =====================================
                # LOG
                # =====================================

                if linha_num % 100000 == 0:

                    print(
                        f"📌 {linha_num} linhas lidas"
                    )

# =====================================
# ÚLTIMO LOTE
# =====================================

if updates:

    execute_batch(

        cursor,

        """

        UPDATE empresas_detalhes

        SET

            razao_social = %s,

            natureza_juridica = %s,

            porte_empresa = %s,

            capital_social = %s

        WHERE cnpj_base = %s

        """,

        updates

    )

    conn.commit()

    total_updates += len(updates)

# =====================================
# FINAL
# =====================================

cursor.close()

conn.close()

print("\n🎯 FINALIZADO")
print(f"✅ TOTAL ATUALIZADO: {total_updates}")