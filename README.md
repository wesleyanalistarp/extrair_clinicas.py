# CallVendas — Gerador de Leads (Receita Federal)

SaaS de geração de leads para prospecção comercial de empresas do setor de saúde (clínicas, hospitais, odontologia, laboratórios de diagnóstico), construído em cima dos dados públicos e abertos de CNPJ da Receita Federal.

- App web: Flask + Postgres (Neon)
- Deploy: Render
- Fonte de dados: [Dados Abertos do CNPJ](https://arquivos.receitafederal.gov.br) da Receita Federal

## Configuração inicial

1. Clonar o projeto e criar o ambiente virtual:
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

2. Criar um arquivo `.env` na raiz do projeto (nunca commitar esse arquivo) com:
   ```
   DATABASE_URL=postgresql://usuario:senha@host/banco?sslmode=require&channel_binding=require
   SECRET_KEY=uma-chave-secreta-qualquer
   ```

3. Rodar o app localmente:
   ```powershell
   python app.py
   ```
   Acessa em `http://127.0.0.1:5000`.

## Atualizando a base com dados novos da Receita Federal

A Receita libera uma base nova (Empresas + Estabelecimentos) por mês. Não vale a pena rodar isso com mais frequência do que isso — os dados na fonte não mudam entre uma liberação e outra.

Antes de rodar tudo, dá pra só checar se já saiu mês novo (rápido, não baixa nada):
```powershell
python atualizar_base_receita.py --listar
```

Se tiver mês novo, roda a sequência completa **nessa ordem**:

```powershell
# 1. Baixa a base mais recente da Receita Federal (pode levar de 1h a várias horas)
python atualizar_base_receita.py

# 2. Importa os leads NOVOS (empresas que ainda não estão no banco) — filtro padrão: SP, BA, MG, PE
python importar_novos_leads.py

# 3. Enriquece os dados de contato/endereço de cada empresa (empresas_detalhes)
python enriquecer_psycopg2.py

# 4. Preenche razão social, natureza jurídica, porte e capital social
python importar_empresas_receita.py

# 5. Recalcula o score de cada lead
python gerar_score_leads.py

# 6. Grava o snapshot do dashboard (totais + data de atualização) — sempre por último
python atualizar_dashboard_snapshot.py
```

Depois disso, confere no `/dashboard` se a "Última atualização" e o "% vs última base" batem com o esperado.

### Scripts de manutenção (uso pontual, não fazem parte do fluxo mensal)

- `corrigir_nome_empresas.py` — corrige o nome de empresas que ficaram sem nome na listagem (`/buscar`), copiando de `empresas_detalhes`. Só precisa rodar se o `/buscar` mostrar "Sem nome" em massa.
- `limpar_leads_fora_estado.py` — remove da base leads de UFs fora do foco comercial (padrão: mantém SP, BA, MG, PE). Útil se o banco no Neon chegar perto do limite de armazenamento do plano.
  ```powershell
  python limpar_leads_fora_estado.py            # simulação (não apaga nada)
  python limpar_leads_fora_estado.py --confirmar # apaga de verdade
  ```
  Depois de rodar com `--confirmar`, roda no SQL Editor do Neon:
  ```sql
  VACUUM ANALYZE empresas;
  VACUUM ANALYZE empresas_detalhes;
  ```

## Estrutura do banco (tabelas principais)

- `empresas` — dados básicos de contato usados na listagem (`/buscar`) e no CRM (status, observação).
- `empresas_detalhes` — dados completos por CNPJ (endereço, razão social, CNAE, score etc.), usados no modal de detalhes (`/empresa/<cnpj>`).
- `dashboard_snapshot` — histórico de totais, usado para calcular o "% vs última base" no dashboard.
- `sistema_info` — guarda a data/hora da última atualização exibida no dashboard.

## Deploy (Render)

O deploy é automático a partir do branch `master` no GitHub. A variável `DATABASE_URL` precisa ser configurada manualmente no painel do Render (Environment) — ela **não** lê o `.env` local. Se a senha do banco for trocada no Neon, atualiza também lá antes do próximo deploy.

## Segurança

- Nunca commitar `.env` nem credenciais direto no código — todos os scripts leem `DATABASE_URL` do `.env` via `python-dotenv`.
- O repositório é público no GitHub — cuidado ao adicionar qualquer arquivo novo com dado sensível.
