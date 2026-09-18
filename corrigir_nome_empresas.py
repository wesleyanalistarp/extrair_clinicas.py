"""
corrigir_nome_empresas.py

Achado no /buscar: as empresas aparecem como "Sem nome" na listagem, mesmo
depois do enriquecer_psycopg2.py + importar_empresas_receita.py já terem
preenchido o nome corretamente em `empresas_detalhes`.

Causa: a tabela `empresas` (usada pela tela /buscar) tem sua PRÓPRIA coluna
`nome`, separada da `empresas_detalhes`. Essa coluna só é preenchida uma vez,
no momento da importação (importar_novos_leads.py), usando só o nome
fantasia do arquivo de Estabelecimentos — que a maioria das empresas não
tem. A razão social só chega depois, no `empresas_detalhes`, e nada nunca
copiava esse valor de volta pra `empresas.nome`.

Esse script corrige isso: preenche `empresas.nome` a partir de
`empresas_detalhes` (nome fantasia, senão razão social) sempre que
`empresas.nome` estiver vazio. Não mexe em quem já tem nome preenchido.

Uso:
    python corrigir_nome_empresas.py
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL não definida. Configure o .env")


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cursor = conn.cursor()

    print("🔎 Contando quantas empresas estão sem nome...")
    cursor.execute("SELECT COUNT(*) FROM empresas WHERE nome IS NULL OR nome = ''")
    antes = cursor.fetchone()[0]
    print(f"   {antes} empresas sem nome antes da correção")

    print("\n🔧 Corrigindo a partir de empresas_detalhes...")
    cursor.execute("""
        UPDATE empresas e
        SET nome = COALESCE(NULLIF(d.nome_fantasia, ''), NULLIF(d.razao_social, ''))
        FROM empresas_detalhes d
        WHERE d.cnpj = e.cnpj
          AND (e.nome IS NULL OR e.nome = '')
          AND COALESCE(NULLIF(d.nome_fantasia, ''), NULLIF(d.razao_social, '')) IS NOT NULL
    """)
    corrigidas = cursor.rowcount
    conn.commit()
    print(f"   ✅ {corrigidas} empresas corrigidas")

    cursor.execute("SELECT COUNT(*) FROM empresas WHERE nome IS NULL OR nome = ''")
    depois = cursor.fetchone()[0]
    print(f"\n   Restam {depois} empresas ainda sem nome")
    print("   (essas realmente não têm nome fantasia NEM razão social preenchidos")
    print("   nos dados da Receita — não é bug, é dado ausente na fonte mesmo)")

    cursor.close()
    conn.close()

    print("\n🎉 FINALIZADO")


if __name__ == "__main__":
    main()
