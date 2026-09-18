"""
atualizar_dashboard_snapshot.py

O dashboard (/dashboard) foi programado pra mostrar "Antes: X / +Y% vs
última base" e "Última atualização: <data>", mas nenhum script do projeto
nunca gravava nada nas tabelas que alimentam isso (dashboard_snapshot e
sistema_info) — só existia o SELECT no app.py, lendo tabelas sempre vazias.
Por isso sempre aparecia "Não definido" e "+0.0%" (comparando o total com
ele mesmo).

Esse script grava um "retrato" (snapshot) do estado atual da base:
- dashboard_snapshot: total de empresas e total com telefone agora
- sistema_info: data/hora desta atualização

Roda ele como ÚLTIMO passo do pipeline, depois do gerar_score_leads.py —
assim, da próxima vez que a base for atualizada de novo, o dashboard vai
comparar com o snapshot de agora e mostrar um "antes x depois" real.

Uso:
    python atualizar_dashboard_snapshot.py
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
    cursor = conn.cursor()

    print("🔎 Calculando totais atuais...")

    cursor.execute("SELECT COUNT(*) FROM empresas")
    total_empresas = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) FROM empresas
        WHERE telefone IS NOT NULL AND telefone != ''
    """)
    total_telefone = cursor.fetchone()[0]

    print(f"   Total de empresas: {total_empresas}")
    print(f"   Com telefone:      {total_telefone}")

    print("\n📸 Gravando snapshot em dashboard_snapshot...")
    cursor.execute("""
        INSERT INTO dashboard_snapshot (total_empresas, total_telefone)
        VALUES (%s, %s)
    """, (total_empresas, total_telefone))

    print("🕒 Gravando data/hora em sistema_info...")
    cursor.execute("""
        INSERT INTO sistema_info (ultima_atualizacao)
        VALUES (NOW())
    """)

    conn.commit()
    cursor.close()
    conn.close()

    print("\n🎉 FINALIZADO — o dashboard já vai mostrar 'Última atualização' certa.")
    print("   O '% vs última base' só fica real de verdade na PRÓXIMA vez que")
    print("   você rodar esse script de novo (ele compara com o snapshot anterior).")


if __name__ == "__main__":
    main()
