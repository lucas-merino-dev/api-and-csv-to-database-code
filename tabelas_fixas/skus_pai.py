import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# ============================
# CONFIGURAÇÃO
# ============================

csv_file = r"G:\Meu Drive\file.csv"
arquivo_registro = r"C:\Users\path"

mysql_user = "user"
mysql_password = "password"
mysql_host = "host"
mysql_port = 3306
mysql_database = "database"
mysql_table = "skus_pai"

def limpar_tabela(engine, nome_tabela):
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {nome_tabela}"))
    print(f"{datetime.now()} Dados da tabela '{nome_tabela}' foram limpos com sucesso!")

def arquivo_atualizado():
    if not os.path.exists(arquivo_registro):
        return True
    with open(arquivo_registro, 'r') as f:
        ultima_execucao = f.read().strip()
    if not ultima_execucao:
        return True
    ultima_data = datetime.fromisoformat(ultima_execucao)
    data_modificacao = datetime.fromtimestamp(os.path.getmtime(csv_file))
    return data_modificacao > ultima_data

if arquivo_atualizado():
    print(f"[{datetime.now()}] Lendo CSV...")
    df = pd.read_csv(csv_file, encoding='utf-8-sig', sep=';')
    print(f"Colunas do CSV: {df.columns}")
    print(df.head())

    # Cria engine de conexão
    engine = create_engine(
        f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}"
    )

    limpar_tabela(engine, mysql_table)
    
    df.to_sql(
        name=mysql_table,
        con=engine,
        if_exists='append',
        index=False,
        chunksize=1000
    )

    with open(arquivo_registro, 'w') as f:
        f.write(datetime.now().isoformat())

    print(f"[{datetime.now()}] Dados inseridos com sucesso!")
else:
    print(f"[{datetime.now()}] Nenhuma atualização detectada, nada inserido.")
