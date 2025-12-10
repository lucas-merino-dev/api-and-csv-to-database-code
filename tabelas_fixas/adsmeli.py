import os
import pandas as pd
import mysql.connector
import numpy as np
from datetime import datetime

# === CONFIGURAÇÕES ===
caminho_csv = r"G:\Meu Drive\file.csv"
arquivo_registro = r"C:\Users\path"

host = 'host'
usuario = 'user'
senha = 'password'
banco = 'database'
tabela = 'metricas_meli_ads'

def inserir_dados():
    print(f"⏳ [{datetime.now()}] Lendo planilha...")
    df = pd.read_csv(caminho_csv, sep=';', encoding='utf-8-sig')
    df.columns = df.columns.str.strip().str.replace('\ufeff', '')
    df = df.replace(['nan', 'None', '-', None], np.nan)

    colunas_numericas = [
        'cpc_ads','ctr_ads','cvr_ads','receita_ads','investimento_ads','acos_ads',
        'kl_ads','receita_vendas_diretas_ads','receita_vendas_indiretas'
    ]
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '.')
            df[col] = df[col].replace('-', np.nan).astype(float)

    df = df.replace({np.nan: None, 'nan': None, 'NaN': None, 'None': None, '-': None})

    conn = mysql.connector.connect(
        host=host,
        user=usuario,
        password=senha,
        database=banco
    )
    cursor = conn.cursor()

    colunas = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    sql = f"INSERT INTO {tabela} ({colunas}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        cursor.execute(sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
    print("Dados inseridos com sucesso!")

def arquivo_atualizado():
    if not os.path.exists(arquivo_registro):
        return True
    with open(arquivo_registro, 'r') as f:
        ultima_execucao = f.read().strip()
    if not ultima_execucao:
        return True
    ultima_data = datetime.fromisoformat(ultima_execucao)
    data_modificacao = datetime.fromtimestamp(os.path.getmtime(caminho_csv))
    return data_modificacao > ultima_data

if arquivo_atualizado():
    inserir_dados()
    with open(arquivo_registro, 'w') as f:
        f.write(datetime.now().isoformat())
else:
    print(f"[{datetime.now()}] Nenhuma atualização detectada, nada inserido.")
