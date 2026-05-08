import os

PASTA = r"C:\extrair_clinicas.py\dados_receita"

bases_encontradas = []

total = 0

for pasta in os.listdir(PASTA):

    caminho_pasta = os.path.join(PASTA, pasta)

    if not os.path.isdir(caminho_pasta):
        continue

    print(f"\n📂 Pasta: {pasta}")

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

                    if len(partes) < 2:
                        continue

                    cnpj_base = (
                        partes[0]
                        .replace('"', '')
                        .strip()
                    )

                    razao = partes[1]

                    print("\n==========================")
                    print("CNPJ BASE:", cnpj_base)
                    print("RAZAO:", razao)
                    print("LINHA:", partes)
                    print("==========================")

                    total += 1

                    if total >= 30:
                        raise SystemExit

                except Exception as e:

                    print("ERRO:", e)