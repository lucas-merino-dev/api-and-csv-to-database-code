import os
import pandas as pd
import mysql.connector
import numpy as np
from datetime import datetime

# === CONFIGURAÇÕES ===
caminho_csv = r'G:\Meu Drive\file.csv'
arquivo_registro = r'C:\Users\path'

host = 'host'
usuario = 'user'
senha = 'password'
banco = 'database'
tabela = 'metricas_meli'

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

def inserir_dados():
    print(f"⏳ [{datetime.now()}] Lendo CSV...")
    df = pd.read_csv(caminho_csv, sep=';', encoding='utf-8-sig')

    df = df.replace({np.nan: None, 'nan': None, 'NaN': None, 'None': None})

    colunas_numericas = [
        'faturamento', 
        'quantidade', 
        'preco_unitario', 
        'porcentagem_participacao', 
        'conversao_visitas_vendas', 
        'conversao_visitas_compradores'
    ]
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(',', '.')
                .replace('None', None)
            )

    if "mes" in df.columns:
        df["mes"] = (
            df["mes"]
            .fillna(0)          # para evitar NaN
            .astype(float)      # garantir que seja número
            .astype(int)        # 12.0 → 12
            .astype(str)        # 12 → "12"
            .str.zfill(2)       # "1" → "01"
        )

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

    print(f"[{datetime.now()}] Dados inseridos com sucesso!")

if arquivo_atualizado():
    inserir_dados()
    with open(arquivo_registro, 'w') as f:
        f.write(datetime.now().isoformat())
else:
    print(f"[{datetime.now()}] Nenhuma atualização detectada, nada inserido.")
