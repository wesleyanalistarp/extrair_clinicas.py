import os
import csv
import psycopg2
from psycopg2 import OperationalError, InterfaceError
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL não definida. Configure o .env")

# ======================================================
# CONEXÃO
# ======================================================

def conectar():

    conn = psycopg2.connect(DATABASE_URL)

    conn.autocommit = False

    return conn


conn = conectar()
cursor = conn.cursor()

# ======================================================
# CORRIGE LINHAS ANTIGAS SEM cnpj_base
# (sem isso, o importar_empresas_receita.py nunca acha essas
# linhas pra preencher razao_social/natureza/porte/capital,
# e a empresa fica pra sempre "sem nome" no site)
# ======================================================

cursor.execute("""
    UPDATE empresas_detalhes
    SET cnpj_base = LEFT(cnpj, 8)
    WHERE cnpj_base IS NULL AND cnpj IS NOT NULL
""")
if cursor.rowcount:
    print(f"🔧 Corrigido cnpj_base em {cursor.rowcount} linhas antigas")
conn.commit()

# ======================================================
# CORRIGE LINHAS ANTIGAS SEM data_inicio_atividade
# (esse campo nunca era gravado aqui, então o "tempo de
# empresa" no modal do site sempre aparecia "-"; a empresas
# já tem essa data certa em empresas.data_inicio)
# ======================================================

cursor.execute("""
    UPDATE empresas_detalhes d
    SET data_inicio_atividade = TO_DATE(e.data_inicio, 'YYYYMMDD')
    FROM empresas e
    WHERE d.cnpj = e.cnpj
      AND d.data_inicio_atividade IS NULL
      AND e.data_inicio ~ '^\\d{8}$'
""")
if cursor.rowcount:
    print(f"🔧 Corrigido data_inicio_atividade em {cursor.rowcount} linhas antigas")
conn.commit()

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

                    data_inicio_raw = linha[10].strip()

                    data_inicio_atividade = None

                    if (
                        data_inicio_raw
                        and data_inicio_raw != "0"
                        and len(data_inicio_raw) == 8
                    ):

                        data_inicio_atividade = (
                            f"{data_inicio_raw[:4]}-"
                            f"{data_inicio_raw[4:6]}-"
                            f"{data_inicio_raw[6:]}"
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
                            cnpj_base,
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

                            matriz_filial,
                            data_inicio_atividade

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
                            %s,

                            %s,
                            %s

                        )

                        ON CONFLICT (cnpj)
                        DO NOTHING

                    """, (

                        cnpj,
                        cnpj[:8],
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

                        linha[1].strip(),
                        data_inicio_atividade

                    ))

                    lote += 1

                    # ======================================================
                    # COMMIT EM LOTES
                    # ======================================================

                    if lote >= 200:

                        conn.commit()

                        print(
                            f"{encontrados} encontrados"
                        )

                        lote = 0

                # ======================================================
                # RECONEXÃO AUTOMÁTICA
                # ======================================================

                except (
                    OperationalError,
                    InterfaceError
                ) as e:

                    print("\nCONEXÃO CAIU:")
                    print(e)

                    try:
                        conn.close()
                    except:
                        pass

                    print("\nReconectando ao banco...")

                    conn = conectar()
                    cursor = conn.cursor()

                    lote = 0

                except Exception as e:

                    print("\nERRO:", e)

                    try:
                        conn.rollback()
                    except:
                        pass

# ======================================================
# COMMIT FINAL
# ======================================================

try:
    conn.commit()
except:
    pass

cursor.close()
conn.close()

print("\nFINALIZADO")
print(f"Total encontrados: {encontrados}")