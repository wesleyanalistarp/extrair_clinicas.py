import os

# =====================================================
# PASTA BASE
# =====================================================

PASTA = r"C:\extrair_clinicas.py\dados_receita"

# =====================================================
# FUNÇÃO ANALISAR
# =====================================================

def analisar_arquivo(caminho_arquivo, nome):

    print("\n" + "=" * 80)
    print(f"📄 ANALISANDO: {nome}")
    print("=" * 80)

    with open(
        caminho_arquivo,
        "r",
        encoding="latin1"
    ) as arquivo:

        for numero_linha, linha in enumerate(arquivo):

            partes = linha.strip().split(";")

            print(f"\n📌 LINHA {numero_linha + 1}")
            print(f"📦 TOTAL COLUNAS: {len(partes)}")

            print("\n🔍 CAMPOS:\n")

            for i, valor in enumerate(partes):

                valor = valor.replace('"', '').strip()

                print(f"[{i}] => {valor}")

            # analisa só primeiras linhas
            if numero_linha >= 2:
                break

# =====================================================
# PROCURA EMPRESAS
# =====================================================

for pasta in os.listdir(PASTA):

    caminho_pasta = os.path.join(PASTA, pasta)

    if not os.path.isdir(caminho_pasta):
        continue

    # =================================================
    # EMPRESAS
    # =================================================

    if "empresa" in pasta.lower():

        for arquivo in os.listdir(caminho_pasta):

            caminho_arquivo = os.path.join(
                caminho_pasta,
                arquivo
            )

            analisar_arquivo(
                caminho_arquivo,
                f"EMPRESAS -> {arquivo}"
            )

            break

    # =================================================
    # ESTABELECIMENTOS
    # =================================================

    if "estabelecimento" in pasta.lower():

        for arquivo in os.listdir(caminho_pasta):

            caminho_arquivo = os.path.join(
                caminho_pasta,
                arquivo
            )

            analisar_arquivo(
                caminho_arquivo,
                f"ESTABELECIMENTOS -> {arquivo}"
            )

            break