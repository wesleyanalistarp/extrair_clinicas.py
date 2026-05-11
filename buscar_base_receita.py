import os

BASE = "44912303"

PASTA = r"C:\extrair_clinicas.py\dados_receita"

encontrou = False

for pasta in os.listdir(PASTA):

    if "empresa" not in pasta.lower():
        continue

    caminho_pasta = os.path.join(PASTA, pasta)

    for arquivo_nome in os.listdir(caminho_pasta):

        if "EMPRECSV" not in arquivo_nome.upper():
            continue

        caminho_arquivo = os.path.join(
            caminho_pasta,
            arquivo_nome
        )

        print(f"\n📂 LENDO: {arquivo_nome}")

        with open(
            caminho_arquivo,
            "r",
            encoding="latin1"
        ) as arquivo:

            for linha in arquivo:

                partes = linha.strip().split(";")

                if len(partes) < 6:
                    continue

                cnpj_base = (
                    partes[0]
                    .replace('"', '')
                    .strip()
                    .zfill(8)
                )

                if cnpj_base == BASE:

                    encontrou = True

                    print("\n🎯 BASE ENCONTRADA\n")

                    print("CNPJ BASE:", partes[0])
                    print("RAZAO:", partes[1])
                    print("NATUREZA:", partes[2])
                    print("CAPITAL:", partes[4])
                    print("PORTE:", partes[5])

                    break

        if encontrou:
            break

    if encontrou:
        break

if not encontrou:

    print("\n❌ BASE NÃO ENCONTRADA NA RECEITA")