import psycopg2
from datetime import datetime

# =====================================================
# CONEXÃO
# =====================================================

DATABASE_URL = "postgresql://neondb_owner:npg_KR5TGagzE1mZ@ep-steep-dust-am57tgsu-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

conn = psycopg2.connect(DATABASE_URL)

cursor = conn.cursor()

# =====================================================
# CRIA COLUNAS CASO NÃO EXISTAM
# =====================================================

try:

    cursor.execute("""

        ALTER TABLE empresas_detalhes
        ADD COLUMN IF NOT EXISTS lead_score INTEGER DEFAULT 0;

    """)

    cursor.execute("""

        ALTER TABLE empresas_detalhes
        ADD COLUMN IF NOT EXISTS lead_classificacao VARCHAR(50);

    """)

    conn.commit()

    print("✅ Colunas criadas/verificadas")

except Exception as e:

    conn.rollback()

    print("❌ Erro criando colunas:", e)

# =====================================================
# BUSCA EMPRESAS
# =====================================================

cursor.execute("""

    SELECT

        cnpj,

        telefone,

        email,

        razao_social,

        capital_social,

        porte_empresa,

        situacao_cadastral,

        data_inicio_atividade

    FROM empresas_detalhes

""")

empresas = cursor.fetchall()

print(f"\n📦 Empresas encontradas: {len(empresas)}")

# =====================================================
# PROCESSAMENTO
# =====================================================

total = 0

for empresa in empresas:

    try:

        cnpj = empresa[0]
        telefone = empresa[1]
        email = empresa[2]
        razao_social = empresa[3]
        capital_social = empresa[4]
        porte = empresa[5]
        situacao = empresa[6]
        data_inicio = empresa[7]

        score = 0

        # =================================================
        # TELEFONE
        # =================================================

        if telefone and str(telefone).strip():

            score += 20

        # =================================================
        # EMAIL
        # =================================================

        if email and str(email).strip():

            score += 20

        # =================================================
        # RAZAO SOCIAL
        # =================================================

        if razao_social and str(razao_social).strip():

            score += 10

        # =================================================
        # SITUAÇÃO
        # =================================================

        if str(situacao).zfill(2) == "02":

            score += 20

        # =================================================
        # CAPITAL SOCIAL
        # =================================================

        try:

            if capital_social:

                capital = float(
                    str(capital_social)
                    .replace(".", "")
                    .replace(",", ".")
                )

                if capital >= 1000000:

                    score += 25

                elif capital >= 100000:

                    score += 15

                elif capital >= 10000:

                    score += 5

        except:
            pass

        # =================================================
        # PORTE
        # =================================================

        if str(porte).zfill(2) == "05":

            score += 10

        elif str(porte).zfill(2) == "03":

            score += 5

        # =================================================
        # TEMPO EMPRESA
        # =================================================

        try:

            if data_inicio:

                if isinstance(data_inicio, str):

                    data_inicio = datetime.strptime(
                        data_inicio,
                        "%Y-%m-%d"
                    ).date()

                anos = (
                    datetime.now().date() - data_inicio
                ).days // 365

                if anos >= 10:

                    score += 30

                elif anos >= 5:

                    score += 20

                elif anos >= 2:

                    score += 10

                else:

                    score -= 10

        except:
            pass

        # =================================================
        # CLASSIFICAÇÃO
        # =================================================

        if score >= 80:

            classificacao = "🔥 Lead Premium"

        elif score >= 60:

            classificacao = "⭐ Lead Quente"

        elif score >= 40:

            classificacao = "🟡 Lead Médio"

        else:

            classificacao = "⚪ Lead Básico"

        # =================================================
        # UPDATE
        # =================================================

        cursor.execute("""

            UPDATE empresas_detalhes
            SET

                lead_score = %s,

                lead_classificacao = %s

            WHERE cnpj = %s

        """, (

            score,

            classificacao,

            cnpj

        ))

        total += 1

        # =================================================
        # LOG
        # =================================================

        if total % 5000 == 0:

            conn.commit()

            print(f"✅ {total} leads processados")

    except Exception as e:

        print(f"❌ Erro no CNPJ {empresa[0]}:", e)

# =====================================================
# FINAL
# =====================================================

conn.commit()

cursor.close()

conn.close()

print("\n🎉 SCORE DE LEADS FINALIZADO")