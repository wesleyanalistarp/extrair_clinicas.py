"""
atualizar_base_receita.py

Baixa a versao MAIS RECENTE dos "Dados Abertos do CNPJ" da Receita Federal
(arquivos Empresas e Estabelecimentos) e organiza em `dados_receita/` no
mesmo formato de pastas que os outros scripts deste projeto ja esperam
(Empresa0..Empresa9, Estabelecimentos0..Estabelecimentos9).

A Receita distribui esses arquivos hoje por um compartilhamento estilo
Nextcloud (WebDAV com token público), não mais por uma pasta HTML comum -
por isso o acesso é via PROPFIND/WebDAV em vez de um índice de diretório.

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
    python atualizar_base_receita.py --listar        # só lista os meses/arquivos disponíveis e sai

Requer: requests  (pip install requests)
"""

import os
import re
import sys
import argparse
import zipfile
import time
from xml.etree import ElementTree

import requests

# Compartilhamento público oficial da Receita Federal para os dados abertos do CNPJ.
# (mesmo mecanismo usado por projetos de referência como rictom/cnpj-sqlite)
SHARE_TOKEN = "YggdBLfdninEJX9"
WEBDAV_BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav"
DAV_FILES_BASE = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/{SHARE_TOKEN}"

DAV_NS = {"d": "DAV:"}

AUTH = (SHARE_TOKEN, "")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
}

PASTA_DESTINO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dados_receita")


def propfind(url, tentativas=3):
    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.request(
                "PROPFIND", url, auth=AUTH, headers={**HEADERS, "Depth": "1"}, timeout=60
            )
            resp.raise_for_status()
            return ElementTree.fromstring(resp.content)
        except (requests.RequestException, ElementTree.ParseError) as e:
            print(f"   ⚠️ Falha no PROPFIND (tentativa {tentativa}/{tentativas}): {e}")
            time.sleep(3)
    raise RuntimeError(f"Não consegui consultar {url} depois de {tentativas} tentativas")


def listar_hrefs(url):
    root = propfind(url)
    hrefs = []
    for resp in root.findall("d:response", DAV_NS):
        href_el = resp.find("d:href", DAV_NS)
        if href_el is not None and href_el.text:
            hrefs.append(href_el.text)
    return hrefs


def descobrir_mes_mais_recente():
    print(f"🔎 Consultando o compartilhamento da Receita Federal (WebDAV)...")
    hrefs = listar_hrefs(WEBDAV_BASE + "/")

    meses = set()
    for h in hrefs:
        m = re.search(r"(\d{4}-\d{2})/?$", h)
        if m:
            meses.add(m.group(1))

    if not meses:
        raise RuntimeError(
            "Não consegui identificar pastas de mês (formato AAAA-MM) na resposta do WebDAV. "
            "O mecanismo de acesso pode ter mudado de novo - avisa que eu ajusto o script."
        )

    mais_recente = sorted(meses)[-1]
    print(f"📅 Mês mais recente disponível: {mais_recente}")
    return mais_recente


def listar_arquivos_do_mes(mes):
    hrefs = listar_hrefs(f"{WEBDAV_BASE}/{mes}/")
    arquivos = []
    for h in hrefs:
        m = re.search(r"/([^/]+\.zip)$", h, re.IGNORECASE)
        if m:
            arquivos.append(m.group(1))
    return sorted(set(arquivos))


def baixar_arquivo(url, destino_zip, tentativas=3):
    for tentativa in range(1, tentativas + 1):
        try:
            with requests.get(url, auth=AUTH, headers=HEADERS, stream=True, timeout=120) as r:
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
                            msg = f"   ⬇️  {baixado / (1024**2):,.0f} MB"
                            if total:
                                msg += f" ({pct:.1f}%)"
                            print(msg)
                            ultimo_print = time.time()
            return True

        except (requests.RequestException, OSError) as e:
            print(f"   ⚠️ Falha na tentativa {tentativa}/{tentativas}: {e}")
            if os.path.exists(destino_zip):
                os.remove(destino_zip)
            time.sleep(5)

    return False


def baixar_e_extrair(nome_zip_remoto, mes, pasta_local_nome):
    url = f"{DAV_FILES_BASE}/{mes}/{nome_zip_remoto}"

    pasta_local = os.path.join(PASTA_DESTINO, pasta_local_nome)
    os.makedirs(pasta_local, exist_ok=True)

    zip_local = os.path.join(pasta_local, nome_zip_remoto)

    print(f"\n📦 {nome_zip_remoto}")

    if not baixar_arquivo(url, zip_local):
        print(f"   ❌ Não foi possível baixar {nome_zip_remoto} — pulando.")
        return False

    print("   📂 Extraindo...")
    try:
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


def atualizar_municipios(mes, arquivos_do_mes):
    candidatos = [a for a in arquivos_do_mes if a.lower().startswith("municipios")]
    if not candidatos:
        print("   ⚠️ Não achei um arquivo de Municípios nesse mês — pulando.")
        return

    print("\n📍 Atualizando tabela de municípios...")
    nome_zip = candidatos[0]
    url = f"{DAV_FILES_BASE}/{mes}/{nome_zip}"
    tmp_zip = os.path.join(PASTA_DESTINO, "_municipios_tmp.zip")

    if not baixar_arquivo(url, tmp_zip):
        print("   ❌ Não foi possível baixar o arquivo de municípios")
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
    parser.add_argument("--listar", action="store_true", help="Só lista os meses/arquivos disponíveis e sai, sem baixar nada")
    args = parser.parse_args()

    os.makedirs(PASTA_DESTINO, exist_ok=True)

    mes = args.mes or descobrir_mes_mais_recente()

    print(f"\n📁 Listando arquivos disponíveis em {mes}...")
    arquivos_do_mes = listar_arquivos_do_mes(mes)

    if not arquivos_do_mes:
        print("❌ Não encontrei nenhum .zip nesse mês. Confere se o mês está certo (--mes AAAA-MM).")
        sys.exit(1)

    print(f"   {len(arquivos_do_mes)} arquivos encontrados:")
    for a in arquivos_do_mes:
        print(f"   - {a}")

    if args.listar:
        return

    empresas_zips = sorted([a for a in arquivos_do_mes if re.match(r"(?i)empresas?\d\.zip$", a)])
    estab_zips = sorted([a for a in arquivos_do_mes if re.match(r"(?i)estabelecimentos?\d\.zip$", a)])

    baixar_empresas = not args.so_estabelecimentos
    baixar_estabelecimentos = not args.so_empresas

    falhas = []

    if baixar_empresas:
        print("\n=== EMPRESAS ===")
        for nome_zip in empresas_zips:
            m = re.search(r"(\d)\.zip$", nome_zip, re.IGNORECASE)
            i = m.group(1) if m else "0"
            ok = baixar_e_extrair(nome_zip, mes, f"Empresa{i}")
            if not ok:
                falhas.append(nome_zip)

    if baixar_estabelecimentos:
        print("\n=== ESTABELECIMENTOS ===")
        for nome_zip in estab_zips:
            m = re.search(r"(\d)\.zip$", nome_zip, re.IGNORECASE)
            i = m.group(1) if m else "0"
            ok = baixar_e_extrair(nome_zip, mes, f"Estabelecimentos{i}")
            if not ok:
                falhas.append(nome_zip)

    if args.municipios:
        atualizar_municipios(mes, arquivos_do_mes)

    print("\n" + "=" * 60)
    if falhas:
        print(f"⚠️ Concluído com falhas em: {', '.join(falhas)}")
        print("   Rode o script de novo depois — ele só baixa de novo o que faltou.")
    else:
        print(f"🎉 Base atualizada com sucesso para o mês de referência {mes}!")
    print("Próximo passo: rode os scripts de importação para levar os dados novos para o Postgres.")


if __name__ == "__main__":
    try:
        import requests  # noqa
    except ImportError:
        print("❌ Falta a lib 'requests'. Rode: pip install requests")
        sys.exit(1)

    main()
