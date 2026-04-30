import os
import pandas as pd
from sqlalchemy import create_engine, text

# ================================
# CONFIGURAÇÃO
# ================================

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL não definida. Configure a variável de ambiente.")

# Corrige padrão antigo (caso aconteça)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Cria engine com pool seguro
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300
)

print("✅ Conectado ao banco!")

# ================================
# FUNÇÃO DE IMPORTAÇÃO
# ================================

def importar_csv(caminho, tabela, encoding="latin1"):
    try:
        print(f"\n📂 Lendo arquivo: {caminho}")
        df = pd.read_csv(caminho, encoding=encoding)

        print(f"🔎 Preview de {tabela}:")
        print(df.head())

        print(f"⬆️ Enviando para tabela: {tabela}")

        df.to_sql(
            tabela,
            engine,
            if_exists="replace",   # depois você pode mudar pra append
            index=False,
            chunksize=1000,
            method="multi"
        )

        print(f"✅ {tabela} importado com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao importar {tabela}: {e}")
        raise

# ================================
# EXECUÇÃO
# ================================

if __name__ == "__main__":
    print("\n🚀 INICIANDO IMPORTAÇÃO...\n")

    importar_csv("municipios.csv", "municipios")
    importar_csv("resultado.csv", "empresas")

    print("\n🎯 IMPORTAÇÃO FINALIZADA COM SUCESSO!")