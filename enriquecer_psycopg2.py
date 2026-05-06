import os
import csv
import psycopg2

# ======================================================
# CONEXÃO
# ======================================================

conn = psycopg2.connect(
    host="ep-steep-dust-am57tgsu.c-5.us-east-1.aws.neon.tech",
    dbname="neondb",
    user="neondb_owner",
    password="npg_KR5TGagzE1mZ",
    sslmode="require"
)

cursor = conn.cursor()

# ======================================================
# BUSCAR CNPJS
# ======================================================

cursor.execute("""
    SELECT cnpj
    FROM empresas
""")

cnpjs_base = set()

for row in cursor.fetchall():

    if row[0]:

        cnpjs_base.add(
            row[0].strip()
        )

print(f"CNPJs carregados: {len(cnpjs_base)}")

# ======================================================
# PASTA
# ======================================================

PASTA = r"dados_receita"

# ======================================================
# CONTROLE
# ======================================================

encontrados = 0
lote = 0

# ======================================================
# PROCESSAMENTO
# ======================================================

for pasta in os.listdir(PASTA):

    caminho_pasta = os.path.join(
        PASTA,
        pasta
    )

    if not os.path.isdir(caminho_pasta):
        continue

    print(f"\nProcessando: {pasta}")

    for arquivo_nome in os.listdir(caminho_pasta):

        arquivo_path = os.path.join(
            caminho_pasta,
            arquivo_nome
        )

        print(f"Lendo: {arquivo_nome}")

        with open(
            arquivo_path,
            mode="r",
            encoding="latin1",
            errors="ignore"
        ) as f:

            leitor = csv.reader(
                f,
                delimiter=";"
            )

            for linha in leitor:

                try:

                    if len(linha) < 28:
                        continue

                    cnpj = (
                        linha[0].strip()
                        + linha[1].strip()
                        + linha[2].strip()
                    )

                    if cnpj not in cnpjs_base:
                        continue

                    encontrados += 1

                    data_raw = linha[6].strip()

                    data_situacao = None

                    if (
                        data_raw
                        and data_raw != "0"
                        and len(data_raw) == 8
                    ):

                        data_situacao = (
                            f"{data_raw[:4]}-"
                            f"{data_raw[4:6]}-"
                            f"{data_raw[6:]}"
                        )

                    logradouro = (
                        f"{linha[13].strip()} "
                        f"{linha[14].strip()}"
                    ).strip()

                    telefone = (
                        f"({linha[21].strip()}) "
                        f"{linha[22].strip()}"
                    ).strip()

                    cursor.execute("""

                        INSERT INTO empresas_detalhes (

                            cnpj,
                            razao_social,
                            nome_fantasia,

                            cep,
                            logradouro,
                            numero,
                            complemento,
                            bairro,

                            municipio,
                            uf,

                            telefone,
                            email,

                            cnae_principal,

                            situacao_cadastral,
                            data_situacao,

                            matriz_filial

                        )

                        VALUES (

                            %s,
                            %s,
                            %s,

                            %s,
                            %s,
                            %s,
                            %s,
                            %s,

                            %s,
                            %s,

                            %s,
                            %s,

                            %s,

                            %s,
                            %s,

                            %s

                        )

                        ON CONFLICT (cnpj)
                        DO NOTHING

                    """, (

                        cnpj,
                        "",
                        linha[4].strip(),

                        linha[18].strip(),
                        logradouro,
                        linha[15].strip(),
                        linha[16].strip(),
                        linha[17].strip(),

                        linha[20].strip(),
                        linha[19].strip(),

                        telefone,
                        linha[27].strip(),

                        linha[11].strip(),

                        linha[5].strip(),
                        data_situacao,

                        linha[1].strip()

                    ))

                    lote += 1

                    if lote >= 200:

                        conn.commit()

                        print(
                            f"{encontrados} encontrados"
                        )

                        lote = 0

                except Exception as e:

                    print("\nERRO:", e)

                    conn.rollback()

conn.commit()

cursor.close()

conn.close()

print("\nFINALIZADO")
print(f"Total encontrados: {encontrados}")