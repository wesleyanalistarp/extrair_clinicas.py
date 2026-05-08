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

BATCH_SIZE = 1000

dados_batch = []

total_lidos = 0

total_atualizados = 0

# =====================================================
# MAPA SITUAÇÃO
# =====================================================

mapa_situacao = {

    "01": "NULA",
    "02": "ATIVA",
    "03": "SUSPENSA",
    "04": "INAPTA",
    "08": "BAIXADA"
}

# =====================================================
# LOOP PASTAS
# =====================================================

for pasta in os.listdir(PASTA):

    if "Estabelecimentos" not in pasta:
        continue

    caminho_pasta = os.path.join(PASTA, pasta)

    if not os.path.isdir(caminho_pasta):
        continue

    print(f"\n📂 Processando: {pasta}")

    # =================================================
    # LOOP ARQUIVOS
    # =================================================

    for arquivo_nome in os.listdir(caminho_pasta):

        if "ESTABELE" not in arquivo_nome.upper():
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

                    if len(partes) < 28:
                        continue

                    # =====================================
                    # CNPJ
                    # =====================================

                    base = partes[0].replace('"', '').strip()

                    filial = partes[1].replace('"', '').strip()

                    dv = partes[2].replace('"', '').strip()

                    cnpj = f"{base}{filial}{dv}"

                    # =====================================
                    # NOME FANTASIA
                    # =====================================

                    nome_fantasia = (
                        partes[4]
                        .replace('"', '')
                        .strip()
                    )

                    # =====================================
                    # SITUAÇÃO
                    # =====================================

                    situacao = (
                        partes[5]
                        .replace('"', '')
                        .strip()
                    )

                    situacao = (
                        partes[5]
                        .replace('"', '')
                        .strip()
                    )[:2]

                    # =====================================
                    # CNAE
                    # =====================================

                    cnae = (
                        partes[11]
                        .replace('"', '')
                        .strip()
                    )

                    # =====================================
                    # ENDEREÇO
                    # =====================================

                    tipo_logradouro = (
                        partes[13]
                        .replace('"', '')
                        .strip()
                    )

                    logradouro = (
                        partes[14]
                        .replace('"', '')
                        .strip()
                    )

                    endereco = (
                        f"{tipo_logradouro} {logradouro}"
                    ).strip()

                    numero = (
                        partes[15]
                        .replace('"', '')
                        .strip()
                    )

                    bairro = (
                        partes[17]
                        .replace('"', '')
                        .strip()
                    )

                    cep = (
                        partes[18]
                        .replace('"', '')
                        .strip()
                    )

                    uf = (
                        partes[19]
                        .replace('"', '')
                        .strip()
                    )

                    # valida UF
                    ufs_validas = {

                        "AC","AL","AP","AM","BA","CE","DF","ES",
                        "GO","MA","MT","MS","MG","PA","PB","PR",
                        "PE","PI","RJ","RN","RS","RO","RR","SC",
                        "SP","SE","TO"
                    }

                    if uf not in ufs_validas:

                        print("\n⚠️ UF INVÁLIDA")
                        print("CNPJ:", cnpj)
                        print("UF:", uf)
                        print("LINHA:", partes)

                        continue

                    # =====================================
                    # TELEFONE
                    # =====================================

                    ddd1 = (
                        partes[21]
                        .replace('"', '')
                        .strip()
                    )

                    telefone1 = (
                        partes[22]
                        .replace('"', '')
                        .strip()
                    )

                    telefone = ""

                    if ddd1 and telefone1:

                        telefone = (
                            f"({ddd1}) {telefone1}"
                        )

                    # =====================================
                    # EMAIL
                    # =====================================

                    email = (
                        partes[27]
                        .replace('"', '')
                        .strip()
                    )

                    # =====================================
                    # MATRIZ/FILIAL
                    # =====================================

                    matriz_filial = filial

                    # =====================================
                    # BATCH
                    # =====================================
                    print("\n===================")
                    print("CNPJ:", cnpj)
                    print("UF:", uf, len(uf))
                    print("SITUACAO:", situacao, len(situacao))
                    print("MATRIZ:", matriz_filial, len(matriz_filial))
                    print("===================")
                    dados_batch.append((

                        nome_fantasia,

                        telefone,

                        email,

                        endereco,

                        numero,

                        bairro,

                        cep,

                        uf,

                        cnae,

                        situacao,

                        matriz_filial,

                        cnpj

                    ))

                    total_lidos += 1

                    # =====================================
                    # LOG
                    # =====================================

                    if total_lidos % 1000 == 0:

                        print(
                            f"📌 {total_lidos} linhas lidas"
                        )

                    # =====================================
                    # EXECUTA LOTE
                    # =====================================

                    if len(dados_batch) >= BATCH_SIZE:

                        execute_batch(

                            cursor,

                            """
                            UPDATE empresas_detalhes
                            SET

                                nome_fantasia = %s,

                                telefone = %s,

                                email = %s,

                                logradouro = %s,

                                numero = %s,

                                bairro = %s,

                                cep = %s,

                                uf = %s,

                                cnae_principal = %s,

                                situacao_cadastral = %s,

                                matriz_filial = %s

                            WHERE cnpj = %s
                            """,

                            dados_batch,

                            page_size=BATCH_SIZE

                        )

                        conn.commit()

                        print(
                            f"✅ LOTE ATUALIZADO | {cursor.rowcount}"
                        )

                        total_atualizados += len(
                            dados_batch
                        )

                        dados_batch.clear()

                except Exception as e:

                    conn.rollback()

                    print("ERRO:", e)

# =====================================================
# RESTANTE
# =====================================================

if dados_batch:

    try:

        execute_batch(

            cursor,

            """
            UPDATE empresas_detalhes
            SET

                nome_fantasia = %s,

                telefone = %s,

                email = %s,

                logradouro = %s,

                numero = %s,

                bairro = %s,

                cep = %s,

                uf = %s,

                cnae_principal = %s,

                situacao_cadastral = %s,

                matriz_filial = %s

            WHERE cnpj = %s
            """,

            dados_batch,

            page_size=BATCH_SIZE

        )

        conn.commit()

        print(
            f"✅ ÚLTIMO LOTE | {cursor.rowcount}"
        )

    except Exception as e:

        conn.rollback()

        print("ERRO FINAL:", e)

# =====================================================
# FINAL
# =====================================================

cursor.close()

conn.close()

print("\n🎉 IMPORTAÇÃO FINALIZADA")