import os
import psycopg2
from psycopg2.extras import execute_batch

# =====================================================
# CONEXÃO NEON
# =====================================================

DATABASE_URL = "postgresql://neondb_owner:npg_KR5TGagzE1mZ@ep-steep-dust-am57tgsu-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

conn = psycopg2.connect(DATABASE_URL)

cursor = conn.cursor()

# =====================================================
# PASTA RECEITA
# =====================================================

PASTA = r"C:\extrair_clinicas.py\dados_receita"

# =====================================================
# CONFIG
# =====================================================

BATCH_SIZE = 5000

dados_batch = []

total_lidos = 0

# =====================================================
# LOOP PASTAS
# =====================================================

for pasta in os.listdir(PASTA):

    # pega somente pastas EMPRESA
    if "empresa" not in pasta.lower():
        continue

    caminho_pasta = os.path.join(PASTA, pasta)

    if not os.path.isdir(caminho_pasta):
        continue

    print(f"\n📂 Processando pasta: {pasta}")

    # =================================================
    # LOOP ARQUIVOS
    # =================================================

    for arquivo_nome in os.listdir(caminho_pasta):

        arquivo_path = os.path.join(
            caminho_pasta,
            arquivo_nome
        )

        if not os.path.isfile(arquivo_path):
            continue

        print(f"📄 Lendo arquivo: {arquivo_nome}")

        with open(
            arquivo_path,
            "r",
            encoding="latin1"
        ) as arquivo:

            for linha in arquivo:

                try:

                    partes = linha.strip().split(";")

                    # =================================
                    # VALIDA ESTRUTURA
                    # =================================

                    if len(partes) < 6:
                        continue

                    # =================================
                    # CAMPOS RECEITA
                    # =================================

                    cnpj_base = (
                        partes[0]
                        .replace('"', '')
                        .strip()
                    )

                    razao_social = (
                        partes[1]
                        .replace('"', '')
                        .strip()
                    )

                    ver_juridico = (
                        partes[2]
                        .replace('"', '')
                        .strip()
                    )

                    capital_social = (
                        partes[4]
                        .replace('"', '')
                        .replace(".", "")
                        .replace(",", ".")
                        .strip()
                    )

                    porte_empresa = (
                        partes[5]
                        .replace('"', '')
                        .strip()
                    )

                    # =================================
                    # LOG PRIMEIRAS LINHAS
                    # =================================

                    if total_lidos < 5:

                        print("\n====================")
                        print("CNPJ BASE:", cnpj_base)
                        print("RAZAO:", razao_social)
                        print("VER JURIDICO:", ver_juridico)
                        print("CAPITAL:", capital_social)
                        print("PORTE:", porte_empresa)
                        print("====================")

                    # =================================
                    # BATCH
                    # =================================

                    dados_batch.append((

                        razao_social,

                        ver_juridico,

                        capital_social,

                        porte_empresa,

                        cnpj_base

                    ))

                    total_lidos += 1

                    # =================================
                    # LOG
                    # =================================

                    if total_lidos % 10000 == 0:

                        print(
                            f"📌 {total_lidos} linhas processadas"
                        )

                    # =================================
                    # EXECUTA LOTE
                    # =================================

                    if len(dados_batch) >= BATCH_SIZE:

                        execute_batch(

                            cursor,

                            """
                            UPDATE empresas_detalhes
                            SET

                                razao_social = %s,

                                ver_juridico = %s,

                                capital_social = %s,

                                porte_empresa = %s

                            WHERE cnpj_base = %s
                            """,

                            dados_batch,

                            page_size=BATCH_SIZE

                        )

                        conn.commit()

                        print(
                            f"✅ LOTE ATUALIZADO | {len(dados_batch)} registros"
                        )

                        dados_batch.clear()

                except Exception as e:

                    conn.rollback()

                    print("\n❌ ERRO:", e)

# =====================================================
# ÚLTIMO LOTE
# =====================================================

if dados_batch:

    try:

        execute_batch(

            cursor,

            """
            UPDATE empresas_detalhes
            SET

                razao_social = %s,

                ver_juridico = %s,

                capital_social = %s,

                porte_empresa = %s

            WHERE cnpj_base = %s
            """,

            dados_batch,

            page_size=BATCH_SIZE

        )

        conn.commit()

        print(
            f"✅ ÚLTIMO LOTE | {len(dados_batch)} registros"
        )

    except Exception as e:

        conn.rollback()

        print("\n❌ ERRO FINAL:", e)

# =====================================================
# FINALIZAÇÃO
# =====================================================

cursor.close()

conn.close()

print("\n🎉 IMPORTAÇÃO EMPRESAS FINALIZADA")