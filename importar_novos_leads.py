"""
importar_novos_leads.py

Lê os arquivos crus de Estabelecimentos em `dados_receita/` (baixados pelo
atualizar_base_receita.py) e insere na tabela `empresas` do Postgres SÓ as
empresas que ainda não estão lá — ou seja, é o script que faltava para
transformar "base nova baixada" em "leads novos no dashboard".

Diferente do filtrar_clinicas.py antigo (que filtrava por palavra no nome
fantasia), este usa o CNAE oficial de saúde — o mesmo critério mais preciso
que já era usado no app_leads_pro.py (o app desktop).

Não sobrescreve nada de quem já está na tabela: carrega os CNPJs já
existentes antes de inserir, então nunca mexe no status/observação que
sua equipe já preencheu pelo CRM.

Uso:
    python importar_novos_leads.py                  # padrão: só SP, BA, MG, PE (foco comercial)
    python importar_novos_leads.py --uf RJ RS        # troca as UFs
    python importar_novos_leads.py --uf              # (--uf sem valores) = Brasil inteiro, todas as UFs
    python importar_novos_leads.py --cnae 8630501 8630502   # troca a lista de CNAEs (padrão = saúde)

⚠️ O padrão mudou para SP/BA/MG/PE (mesmo foco do filtrar_clinicas.py original).
Uma importação nacional sem filtro já estourou uma vez o limite de 512 MB do
plano gratuito do Neon — se quiser Brasil inteiro de novo, use "--uf" (sem
nenhum valor depois) de propósito, sabendo que isso deixa a base bem maior.
"""

import os
import argparse
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL não definida. Configure o .env")

PASTA_DADOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dados_receita")

# mesmos códigos usados no app_leads_pro.py (clínicas / hospitais / odonto / diagnóstico)
CNAES_SAUDE_PADRAO = [
    "8630501", "8630502", "8630503",
    "8640201", "8640202", "8640203",
    "8650001", "8650002", "8650003",
]

BATCH_SIZE = 5000

# foco comercial padrão (mesma restrição do filtrar_clinicas.py original) —
# evita reimportar o Brasil inteiro sem querer e estourar o limite do banco de novo
ESTADOS_PADRAO = ["SP", "BA", "MG", "PE"]

UFS_VALIDAS = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES",
    "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR",
    "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
}


def carregar_cnpjs_existentes(cursor):
    print("🔎 Carregando CNPJs já existentes na tabela 'empresas'...")
    cursor.execute("SELECT cnpj FROM empresas")
    existentes = {row[0].strip() for row in cursor.fetchall() if row[0]}
    print(f"   {len(existentes)} CNPJs já cadastrados")
    return existentes


def processar(pasta_estabelecimentos, cnaes, ufs, existentes, novos_batch, cursor, conn, stats):
    for arquivo_nome in os.listdir(pasta_estabelecimentos):
        arquivo_path = os.path.join(pasta_estabelecimentos, arquivo_nome)
        if not os.path.isfile(arquivo_path):
            continue

        print(f"📄 Lendo: {arquivo_nome}")

        with open(arquivo_path, "r", encoding="latin1", errors="ignore") as arquivo:
            for linha in arquivo:
                partes = linha.strip().split(";")

                if len(partes) < 28:
                    continue

                cnae_principal = partes[11].replace('"', '').strip()
                if cnae_principal not in cnaes:
                    continue

                uf = partes[19].replace('"', '').strip()

                if uf not in UFS_VALIDAS:
                    # linha com CSV desalinhado (campo de endereço com ; embutido etc.)
                    # ou lixo de fim de arquivo - descarta em vez de derrubar o lote inteiro
                    stats["uf_invalida"] += 1
                    continue

                if ufs and uf not in ufs:
                    continue

                cnpj = (
                    partes[0].replace('"', '').strip()
                    + partes[1].replace('"', '').strip()
                    + partes[2].replace('"', '').strip()
                )

                if len(cnpj) != 14 or not cnpj.isdigit():
                    stats["cnpj_invalido"] += 1
                    continue

                stats["candidatos"] += 1

                if cnpj in existentes:
                    stats["ja_existia"] += 1
                    continue

                nome_fantasia = partes[4].replace('"', '').strip()
                razao_social = ""  # não vem no arquivo de Estabelecimentos
                municipio = partes[20].replace('"', '').strip()
                data_inicio = partes[10].replace('"', '').strip()

                ddd1 = partes[21].replace('"', '').strip()
                tel1 = partes[22].replace('"', '').strip()
                ddd2 = partes[23].replace('"', '').strip() if len(partes) > 23 else ""
                tel2 = partes[24].replace('"', '').strip() if len(partes) > 24 else ""

                telefone = f"{ddd1}{tel1}" if ddd1 and tel1 else ""
                telefone2 = f"{ddd2}{tel2}" if ddd2 and tel2 else ""

                if not telefone and not telefone2:
                    stats["sem_telefone"] += 1
                    continue

                novos_batch.append((
                    cnpj, nome_fantasia or razao_social, uf, municipio,
                    data_inicio, telefone, telefone2
                ))
                existentes.add(cnpj)  # evita duplicar se o mesmo cnpj aparecer 2x nos arquivos
                stats["inseridos"] += 1

                if len(novos_batch) >= BATCH_SIZE:
                    if inserir_lote(cursor, conn, novos_batch):
                        print(f"   ✅ {stats['inseridos']} leads novos inseridos até agora")
                    else:
                        stats["inseridos"] -= len(novos_batch)
                        stats["lote_com_erro"] += len(novos_batch)
                    novos_batch.clear()


def inserir_lote(cursor, conn, lote):
    """Insere um lote. Se der erro (ex: dado sujo que passou pelas validações),
    descarta só esse lote e deixa o resto do processamento continuar — na
    próxima vez que o script rodar, esses CNPJs (que não foram inseridos)
    são tentados de novo naturalmente."""
    try:
        execute_values(
            cursor,
            """
            INSERT INTO empresas (cnpj, nome, uf, municipio, data_inicio, telefone, telefone2)
            VALUES %s
            """,
            lote,
        )
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f"   ⚠️ Lote de {len(lote)} leads falhou e foi descartado: {e}")
        print(f"      (CNPJs desse lote: {[l[0] for l in lote[:5]]}...)")
        return False


def main():
    parser = argparse.ArgumentParser(description="Importa leads NOVOS (que ainda não existem) da base atualizada da Receita")
    parser.add_argument("--uf", nargs="*", default=ESTADOS_PADRAO, help="Lista de UFs para filtrar. Padrão: SP BA MG PE. Use '--uf' sem valores para trazer todas as UFs (Brasil inteiro).")
    parser.add_argument("--cnae", nargs="*", default=CNAES_SAUDE_PADRAO, help="Lista de códigos CNAE para filtrar. Padrão: CNAEs de saúde.")
    args = parser.parse_args()

    ufs = set(u.upper() for u in args.uf)
    cnaes = set(args.cnae)

    if not os.path.isdir(PASTA_DADOS):
        raise SystemExit(f"❌ Pasta não encontrada: {PASTA_DADOS} — roda o atualizar_base_receita.py primeiro.")

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    existentes = carregar_cnpjs_existentes(cursor)

    stats = {
        "candidatos": 0, "ja_existia": 0, "sem_telefone": 0, "inseridos": 0,
        "uf_invalida": 0, "cnpj_invalido": 0, "lote_com_erro": 0,
    }
    novos_batch = []

    for pasta in sorted(os.listdir(PASTA_DADOS)):
        if "estabelecimento" not in pasta.lower():
            continue

        caminho_pasta = os.path.join(PASTA_DADOS, pasta)
        if not os.path.isdir(caminho_pasta):
            continue

        print(f"\n📂 Processando pasta: {pasta}")
        processar(caminho_pasta, cnaes, ufs, existentes, novos_batch, cursor, conn, stats)
        conn.commit()

    if novos_batch:
        if not inserir_lote(cursor, conn, novos_batch):
            stats["inseridos"] -= len(novos_batch)
            stats["lote_com_erro"] += len(novos_batch)
        conn.commit()

    cursor.close()
    conn.close()

    print("\n" + "=" * 60)
    print(f"🔎 Candidatos que bateram no CNAE/UF: {stats['candidatos']}")
    print(f"⏭️  Já existiam na tabela empresas:    {stats['ja_existia']}")
    print(f"📵 Descartados por falta de telefone:  {stats['sem_telefone']}")
    print(f"⚠️  Descartados por UF inválida (linha suja): {stats['uf_invalida']}")
    print(f"⚠️  Descartados por CNPJ inválido (linha suja): {stats['cnpj_invalido']}")
    print(f"⚠️  Perdidos em lotes com erro:        {stats['lote_com_erro']}")
    print(f"🎉 Leads NOVOS inseridos:              {stats['inseridos']}")
    print("\nPróximo passo: roda o enriquecer_psycopg2.py e o gerar_score_leads.py")
    print("pra completar dados desses leads novos e calcular o score deles.")


if __name__ == "__main__":
    main()
