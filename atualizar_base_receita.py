"""
atualizar_base_receita.py

Baixa a versao MAIS RECENTE dos "Dados Abertos do CNPJ" da Receita Federal
(arquivos Empresas e Estabelecimentos) e organiza em `dados_receita/` no
mesmo formato de pastas que os outros scripts deste projeto ja esperam
(Empresa0..Empresa9, Estabelecimentos0..Estabelecimentos9).

⚠️ AVISO DE TAMANHO / TEMPO
O conjunto completo (Empresas + Estabelecimentos, 10 partes cada) soma
dezenas de GB depois de descompactado (o Estabelecimentos sozinho passa de
60 GB). Rode isso com boa conexão, espaço livre em disco (reserve pelo
menos 80-100 GB no drive C:) e tempo disponível — pode levar de 1 a
várias horas dependendo da sua internet. O script apaga o .zip depois de
extrair cada parte para não duplicar espaço em disco.

Uso:
    python atualizar_base_receita.py                # baixa Empresas + Estabelecimentos (mais recente)
    python atualizar_base_receita.py --so-empresas   # baixa só Empresas
    python atualizar_base_receita.py --mes 2026-08   # força um mês específico em vez do mais recente
    python atualizar_base_receita.py --municipios    # também atualiza o F.K03200$Z...MUNICCSV

Requer: requests  (pip install requests)
"""

import os
import re
import sys
import argparse
import zipfile
import shutil
import time
from urllib.parse import urljoin

import requests

BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
PASTA_DESTINO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dados_receita")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; atualizador-leads/1.0)"
}


def listar_links(url):
    """Retorna a lista de hrefs encontrados numa página de listagem (Apache-style index)."""
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return re.findall(r'href="([^"]+)"', resp.text)


def descobrir_mes_mais_recente():
    print(f"🔎 Consultando índice: {BASE_URL}")
    links = listar_links(BASE_URL)

    meses = sorted(
        {l.strip("/") for l in links if re.fullmatch(r"\d{4}-\d{2}/?", l)}
    )

    if not meses:
        raise RuntimeError(
            "Não consegui identificar pastas de mês (formato AAAA-MM) no índice. "
            "O layout do site pode ter mudado — acesse "
            f"{BASE_URL} manualmente para conferir."
        )

    mais_recente = meses[-1]
    print(f"📅 Mês mais recente disponível: {mais_recente}")
    return mais_recente


def baixar_arquivo(url, destino_zip, tentativas=3):
    for tentativa in range(1, tentativas + 1):
        try:
            with requests.get(url, headers=HEADERS, stream=True, timeout=120) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))
                baixado = 0
                ultimo_print = time.time()

                with open(destino_zip, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        f.write(chunk)
                        baixado += len(chunk)

                        if time.time() - ultimo_print > 5:
                            pct = (baixado / total * 100) if total else 0
                            print(
                                f"   ⬇️  {baixado / (1024**2):,.0f} MB "
                                f"({pct:.1f}%)" if total else
                                f"   ⬇️  {baixado / (1024**2):,.0f} MB"
                            )
                            ultimo_print = time.time()
            return True

        except (requests.RequestException, OSError) as e:
            print(f"   ⚠️ Falha na tentativa {tentativa}/{tentativas}: {e}")
            if os.path.exists(destino_zip):
                os.remove(destino_zip)
            time.sleep(5)

    return False


def baixar_e_extrair(tipo, indice, mes, pasta_local_nome):
    """tipo: 'Empresas' ou 'Estabelecimentos'; indice: 0-9"""
    nome_zip_remoto = f"{tipo}{indice}.zip"
    url = urljoin(f"{BASE_URL}{mes}/", nome_zip_remoto)

    pasta_local = os.path.join(PASTA_DESTINO, pasta_local_nome)
    os.makedirs(pasta_local, exist_ok=True)

    zip_local = os.path.join(pasta_local, nome_zip_remoto)

    print(f"\n📦 {nome_zip_remoto}")
    print(f"   URL: {url}")

    if not baixar_arquivo(url, zip_local):
        print(f"   ❌ Não foi possível baixar {nome_zip_remoto} — pulando.")
        return False

    print("   📂 Extraindo...")
    try:
        # limpa CSVs antigos dessa pasta antes de extrair os novos
        for antigo in os.listdir(pasta_local):
            if antigo != nome_zip_remoto:
                os.remove(os.path.join(pasta_local, antigo))

        with zipfile.ZipFile(zip_local, "r") as z:
            z.extractall(pasta_local)

    except zipfile.BadZipFile:
        print(f"   ❌ Zip corrompido: {nome_zip_remoto}")
        os.remove(zip_local)
        return False

    os.remove(zip_local)  # não guarda o zip, só o CSV extraído
    print(f"   ✅ {pasta_local_nome} atualizado")
    return True


def atualizar_municipios(mes):
    print("\n📍 Atualizando tabela de municípios...")
    url = urljoin(f"{BASE_URL}{mes}/", "Municipios.zip")
    tmp_zip = os.path.join(PASTA_DESTINO, "_municipios_tmp.zip")

    if not baixar_arquivo(url, tmp_zip):
        print("   ❌ Não foi possível baixar Municipios.zip")
        return

    destino = os.path.dirname(PASTA_DESTINO)  # raiz do projeto, onde app.py espera o arquivo
    with zipfile.ZipFile(tmp_zip, "r") as z:
        z.extractall(destino)

    os.remove(tmp_zip)
    print("   ✅ Municípios atualizados")


def main():
    parser = argparse.ArgumentParser(description="Atualiza a base de dados abertos do CNPJ (Receita Federal)")
    parser.add_argument("--mes", help="Força um mês específico (formato AAAA-MM) em vez do mais recente")
    parser.add_argument("--so-empresas", action="store_true", help="Baixa somente os arquivos de Empresas")
    parser.add_argument("--so-estabelecimentos", action="store_true", help="Baixa somente os arquivos de Estabelecimentos")
    parser.add_argument("--municipios", action="store_true", help="Também atualiza o arquivo de municípios")
    args = parser.parse_args()

    os.makedirs(PASTA_DESTINO, exist_ok=True)

    mes = args.mes or descobrir_mes_mais_recente()

    baixar_empresas = not args.so_estabelecimentos
    baixar_estabelecimentos = not args.so_empresas

    falhas = []

    if baixar_empresas:
        print("\n=== EMPRESAS ===")
        for i in range(10):
            ok = baixar_e_extrair("Empresas", i, mes, f"Empresa{i}")
            if not ok:
                falhas.append(f"Empresas{i}")

    if baixar_estabelecimentos:
        print("\n=== ESTABELECIMENTOS ===")
        for i in range(10):
            ok = baixar_e_extrair("Estabelecimentos", i, mes, f"Estabelecimentos{i}")
            if not ok:
                falhas.append(f"Estabelecimentos{i}")

    if args.municipios:
        atualizar_municipios(mes)

    print("\n" + "=" * 60)
    if falhas:
        print(f"⚠️ Concluído com falhas em: {', '.join(falhas)}")
        print("   Rode o script de novo depois — ele só baixa de novo o que faltou.")
    else:
        print(f"🎉 Base atualizada com sucesso para o mês de referência {mes}!")
    print("Próximo passo: rode os scripts de importação "
          "(importar_empresas_receita.py, enriquecer_psycopg2.py, gerar_score_leads.py) "
          "para levar os dados novos para o Postgres.")


if __name__ == "__main__":
    try:
        import requests  # noqa
    except ImportError:
        print("❌ Falta a lib 'requests'. Rode: pip install requests")
        sys.exit(1)

    main()
