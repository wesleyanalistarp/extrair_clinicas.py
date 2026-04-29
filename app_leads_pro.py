import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import os
from unidecode import unidecode

# CNAEs ampliados de saúde
CNAES_SAUDE = [
    "8630501","8630502","8630503",
    "8640201","8640202","8640203",
    "8650001","8650002","8650003"
]

dados_final = None

def normalizar(texto):
    if pd.isna(texto):
        return ""
    return unidecode(str(texto)).upper().strip()

def selecionar_pasta():
    caminho = filedialog.askdirectory()
    entry_pasta.delete(0, tk.END)
    entry_pasta.insert(0, caminho)

def buscar():
    global dados_final

    pasta = entry_pasta.get()
    cidade = normalizar(entry_cidade.get())
    uf = entry_uf.get().upper()
    data_min = entry_data.get()

    try:
        status_label.config(text="Processando...", fg="orange")
        app.update_idletasks()

        df_est = carregar("Estabelecimentos", pasta)
        df_est = tratar_est(df_est, cidade, uf, data_min)

        df_emp = carregar("Empresas", pasta)
        df_emp = tratar_emp(df_emp)

        df = df_est.merge(df_emp, on="cnpj_basico", how="left")

        df = limpar_dados(df)

        dados_final = df

        status_label.config(text=f"{len(df)} leads encontrados", fg="green")

    except Exception as e:
        messagebox.showerror("Erro", str(e))

def carregar(tipo, pasta):
    arquivos = [f for f in os.listdir(pasta) if tipo in f]

    dfs = []
    for arq in arquivos:
        caminho = os.path.join(pasta, arq)
        df = pd.read_csv(caminho, sep=";", encoding="latin1", header=None, dtype=str)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

def tratar_est(df, cidade, uf, data_min):
    df.columns = [
        "cnpj_basico","cnpj_ordem","cnpj_dv","matriz",
        "nome_fantasia","situacao","data_situacao","motivo",
        "cidade_ext","pais","data_inicio","cnae_principal",
        "cnae_sec","tipo_log","logradouro","numero",
        "comp","bairro","cep","uf","municipio",
        "ddd1","tel1","ddd2","tel2","ddd_fax","fax",
        "email","sit_especial","data_especial"
    ]

    df["cnpj"] = df["cnpj_basico"] + df["cnpj_ordem"] + df["cnpj_dv"]
    df["data_inicio"] = pd.to_datetime(df["data_inicio"], errors="coerce")

    df["municipio_norm"] = df["municipio"].apply(normalizar)

    df = df[df["cnae_principal"].isin(CNAES_SAUDE)]

    if cidade:
        df = df[df["municipio_norm"].str.contains(cidade)]

    if uf:
        df = df[df["uf"] == uf]

    if data_min:
        df = df[df["data_inicio"] >= data_min]

    return df

def tratar_emp(df):
    df.columns = [
        "cnpj_basico","razao_social","natureza",
        "qualificacao","capital","porte","ente"
    ]
    return df[["cnpj_basico","razao_social"]]

def limpar_dados(df):
    df["telefone"] = df["ddd1"].fillna("") + df["tel1"].fillna("")
    df["telefone"] = df["telefone"].str.replace(r"\D", "", regex=True)

    df = df.drop_duplicates(subset="cnpj")

    df = df[df["cnpj"].notna()]

    return df

def exportar_excel():
    if dados_final is None or dados_final.empty:
        messagebox.showwarning("Aviso", "Sem dados")
        return

    caminho = filedialog.asksaveasfilename(defaultextension=".xlsx")

    if not caminho:
        return

    with pd.ExcelWriter(caminho, engine="openpyxl") as writer:

        # 📋 dados principais
        dados_final.to_excel(writer, index=False, sheet_name="Leads")

        # 📊 resumo por bairro
        resumo_bairro = dados_final.groupby("bairro").size().reset_index(name="total")
        resumo_bairro.to_excel(writer, index=False, sheet_name="Por Bairro")

        # 📊 resumo por CNAE
        resumo_cnae = dados_final.groupby("cnae_principal").size().reset_index(name="total")
        resumo_cnae.to_excel(writer, index=False, sheet_name="Por CNAE")

    messagebox.showinfo("Sucesso", "Relatório Excel gerado!")

# UI
app = tk.Tk()
app.title("🚀 Leads Inteligentes - Clínicas")
app.geometry("420x400")

tk.Label(app, text="Pasta dos dados").pack()
entry_pasta = tk.Entry(app, width=40)
entry_pasta.pack()

tk.Button(app, text="Selecionar Pasta", command=selecionar_pasta).pack(pady=5)

tk.Label(app, text="Cidade").pack()
entry_cidade = tk.Entry(app)
entry_cidade.pack()

tk.Label(app, text="UF").pack()
entry_uf = tk.Entry(app)
entry_uf.pack()

tk.Label(app, text="Data mínima").pack()
entry_data = tk.Entry(app)
entry_data.insert(0, "2026-04-01")
entry_data.pack()

tk.Button(app, text="🔍 Buscar Leads", command=buscar).pack(pady=10)

tk.Button(app, text="📊 Exportar Excel Profissional", command=exportar_excel).pack()

status_label = tk.Label(app, text="Pronto", fg="blue")
status_label.pack(pady=20)

app.mainloop()