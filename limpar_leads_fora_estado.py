"""
limpar_leads_fora_estado.py

O banco no Neon (plano gratuito) estourou o limite de 512 MB depois da
importação nacional de ~550 mil leads novos (importar_novos_leads.py sem
filtro de UF). Esse script apaga da base os leads de fora do foco comercial
para liberar espaço, e volta a aplicar a mesma restrição de estados que já
existia no filtrar_clinicas.py original (SP, BA, MG, PE).

Só mexe nas tabelas `empresas` e `empresas_detalhes` — nunca apaga por CNAE
nem por status de CRM, só por UF. Roda em modo simulação por padrão (não
apaga nada até você confirmar com --confirmar).

Uso:
    python limpar_leads_fora_estado.py                # simulação: só mostra quantos seriam apagados
    python limpar_leads_fora_estado.py --confirmar     # apaga de verdade
    python limpar_leads_fora_estado.py --manter SP RJ --confirmar   # muda os estados mantidos
"""

import os
import argparse
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL não definida. Configure o .env")

ESTADOS_PADRAO = ["SP", "BA", "MG", "PE"]


def main():
    parser = argparse.ArgumentParser(
        description="Remove leads de UFs fora do foco comercial para liberar espaço no banco"
    )
    parser.add_argument(
        "--manter", nargs="*", default=ESTADOS_PADRAO,
        help="UFs que devem ficar na base (padrão: SP BA MG PE)"
    )
    parser.add_argument(
        "--confirmar", action="store_true",
        help="Executa a exclusão de verdade (sem essa flag, só mostra a simulação)"
    )
    args = parser.parse_args()

    manter = tuple(sorted(set(u.upper() for u in args.manter)))

    if not manter:
        raise SystemExit("❌ --manter não pode ficar vazio (isso apagaria a base inteira).")

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM empresas")
    total_geral = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM empresas WHERE uf NOT IN %s", (manter,))
    total_apagar = cursor.fetchone()[0]

    print("=" * 60)
    print(f"Total atual na tabela 'empresas':     {total_geral}")
    print(f"Estados que vão ficar:                {', '.join(manter)}")
    print(f"Leads a apagar (fora desses estados): {total_apagar}")
    print(f"Leads que vão ficar:                  {total_geral - total_apagar}")
    print("=" * 60)

    if not args.confirmar:
        print("\n⚠️  Modo simulação — nada foi apagado.")
        print("   Rode de novo com --confirmar para executar a exclusão de verdade.")
        cursor.close()
        conn.close()
        return

    print("\n🗑️  Apagando registros de 'empresas_detalhes' (leads fora do filtro)...")
    cursor.execute(
        """
        DELETE FROM empresas_detalhes
        WHERE cnpj IN (SELECT cnpj FROM empresas WHERE uf NOT IN %s)
        """,
        (manter,),
    )
    print(f"   {cursor.rowcount} linhas removidas de empresas_detalhes")
    conn.commit()

    print("🗑️  Apagando registros de 'empresas'...")
    cursor.execute("DELETE FROM empresas WHERE uf NOT IN %s", (manter,))
    print(f"   {cursor.rowcount} linhas removidas de empresas")
    conn.commit()

    cursor.close()
    conn.close()

    print("\n✅ Limpeza concluída.")
    print("\nPróximo passo (importante): abre o SQL Editor do Neon (console.neon.tech)")
    print("e roda:")
    print("    VACUUM ANALYZE empresas;")
    print("    VACUUM ANALYZE empresas_detalhes;")
    print("\n(Isso não é VACUUM FULL de propósito — VACUUM FULL precisa de espaço livre")
    print("extra pra reescrever a tabela inteira, e o banco já está no limite. O VACUUM")
    print("normal libera o espaço das linhas apagadas pro Postgres reaproveitar, e o")
    print("Neon também reduz o tamanho reportado do projeto sozinho depois de um tempo.)")
    print("\nDepois disso, roda o enriquecer_psycopg2.py de novo pra terminar o backfill")
    print("do cnpj_base que tinha travado por falta de espaço.")


if __name__ == "__main__":
    main()
