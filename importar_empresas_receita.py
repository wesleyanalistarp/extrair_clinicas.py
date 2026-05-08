import os
import psycopg2
from psycopg2.extras import execute_batch

# ==========================================
# CONEXÃO
# ==========================================

DATABASE_URL = "postgresql://neondb_owner:npg_KR5TGagzE1mZ@ep-steep-dust-am57tgsu-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

conn = psycopg2.connect(DATABASE_URL)

cursor = conn.cursor()

# ==========================================
# PASTA RECEITA
# ==========================================

PASTA = r"C:\extrair_clinicas.py\dados_receita"

# ==========================================
# CONFIG
# ==========================================

BATCH_SIZE = 10

dados_batch = []

total_lidos = 0

total_atualizados = 0

# ==========================================
# BASE TESTE
# ==========================================

BASE_TESTE = "05738175"

# ==========================================
# LOOP PASTAS
# ==========================================

for pasta in os.listdir(PASTA):

    caminho_pasta = os.path.join(PASTA, pasta)

    if not os.path.isdir(caminho_pasta):
        continue

    print(f"\n📂 Processando pasta: {pasta}")

    # ======================================
    # LOOP ARQUIVOS
    # ======================================

    for arquivo_nome in os.listdir(caminho_pasta):

        if "EMPRECSV" not in arquivo_nome.upper():
            continue

        arquivo_path = os.path.join(
            caminho_pasta,
            arquivo_nome
        )

        print(f"📄 Lendo: {arquivo_nome}")

        with open(
            arquivo_path,
            "r",
            encoding="latin1"
        ) as arquivo:

            for linha in arquivo:

                try:

                    partes = linha.strip().split(";")

                    # ==================================
                    # LINHA INVÁLIDA
                    # ==================================

                    if len(partes) < 2:
                        continue

                    # ==================================
                    # CNPJ BASE
                    # ==================================

                    cnpj_base = (
                        partes[0]
                        .strip()
                        .zfill(8)
                    )

                    if not cnpj_base:
                        continue

                    # ==================================
                    # DEBUG MATCH
                    # ==================================

                    if cnpj_base == BASE_TESTE:

                        print("\n🎯 BASE ENCONTRADA !!!")

                        print(partes)

                    # ==================================
                    # RAZÃO SOCIAL
                    # ==================================

                    razao_social = None

                    if len(partes) > 1:

                        razao_social = (
                            partes[1]
                            .replace('"', '')
                            .strip()
                        )

                    # ==================================
                    # NATUREZA JURÍDICA
                    # ==================================

                    natureza_juridica = None

                    if len(partes) > 2:

                        natureza_juridica = (
                            partes[2]
                            .replace('"', '')
                            .strip()
                        )

                    # ==================================
                    # CAPITAL SOCIAL
                    # ==================================

                    capital_social = None

                    if len(partes) > 4:

                        valor = (
                            partes[4]
                            .replace('"', '')
                            .replace("'", '')
                            .strip()
                        )

                        if valor:

                            try:

                                valor = valor.replace(",", ".")

                                capital_social = float(valor)

                            except:

                                capital_social = None

                    # ==================================
                    # PORTE
                    # ==================================

                    porte_empresa = None

                    if len(partes) > 5:

                        porte_empresa = (
                            partes[5]
                            .replace('"', '')
                            .strip()
                        )

                    # ==================================
                    # BATCH
                    # ==================================

                    dados_batch.append((

                        razao_social,

                        natureza_juridica,

                        capital_social,

                        porte_empresa,

                        cnpj_base

                    ))

                    total_lidos += 1

                    # ==================================
                    # LOG
                    # ==================================

                    if total_lidos % 10 == 0:

                        print(
                            f"📌 {total_lidos} linhas lidas"
                        )

                    # ==================================
                    # EXECUTA LOTE
                    # ==================================

                    if len(dados_batch) >= BATCH_SIZE:

                        execute_batch(

                            cursor,

                            """
                            UPDATE empresas_detalhes
                            SET

                                razao_social = %s,

                                natureza_juridica = %s,

                                capital_social = %s,

                                porte_empresa = %s

                            WHERE LEFT(
                                empresas_detalhes.cnpj,
                                8
                            ) = %s
                            """,

                            dados_batch,

                            page_size=BATCH_SIZE

                        )

                        conn.commit()

                        print(
                            f"LINHAS AFETADAS: {cursor.rowcount}"
                        )

                        total_atualizados += len(dados_batch)

                        print(
                            f"✅ LOTE ATUALIZADO ({total_atualizados})"
                        )

                        dados_batch.clear()

                except Exception as e:

                    conn.rollback()

                    print("ERRO:", e)

# ==========================================
# RESTANTE
# ==========================================

if dados_batch:

    try:

        execute_batch(

            cursor,

            """
            UPDATE empresas_detalhes
            SET

                razao_social = %s,

                natureza_juridica = %s,

                capital_social = %s,

                porte_empresa = %s

            WHERE LEFT(
                empresas_detalhes.cnpj,
                8
            ) = %s
            """,

            dados_batch,

            page_size=BATCH_SIZE

        )

        conn.commit()

        total_atualizados += len(dados_batch)

        print(
            f"✅ ÚLTIMO LOTE ({total_atualizados})"
        )

    except Exception as e:

        conn.rollback()

        print("ERRO FINAL:", e)

# ==========================================
# FINAL
# ==========================================

cursor.close()

conn.close()

print("\n🎉 IMPORTAÇÃO FINALIZADA")