import requests
import time
from datetime import datetime, timedelta, timezone
import mysql.connector
from decimal import Decimal
from multiprocessing import Process
import logging
import traceback

# =========================================================
# CONFIGURAÇÕES
# =========================================================

# --- Tega ---
X_SAAS_LICENCA_ID = "91"
X_API_KEY = "token_tega"
URL_TEGA_AUTH = "https://sistema.tegasaas.com.br/tegasaas/auth/generate"
URL_TEGA_FATURAMENTO = "https://sistema.tegasaas.com.br/tegasaas/v1/faturamentoano?Ano=2025"
URL_TEGA_LOTE_ABERTO = "https://sistema.tegasaas.com.br/tegasaas/v1/lotesabertos"
URL_TEGA_PRODUCAO = "https://sistema.tegasaas.com.br/tegasaas/v1/producaolote"
URL_TEGA_NF = "https://sistema.tegasaas.com.br/tegasaas/v1/fiscal/notasfiscais"

# --- AnyMarket ---
ANYMARKET_CONFIG = {
    'base_url': 'https://api.anymarket.com.br/v2',
    'empresas': {
        'empresa1': {
            'nome': 'Empresa 1',
            'gumga_token': "token_any1"
        },
        'empresa2': {
            'nome': 'Empresa 2',
            'gumga_token': "token_any2"
        },
        'empresa3': {
            'nome': 'Empresa 3',
            'gumga_token': "token_any3"
        },
        'empresa4': {
            'nome': 'Empresa 4',
            'gumga_token': "token_any4"
        }
    }
}

# --- MySQL ---
MYSQL_HOST = "host"
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_DB = "database"
MYSQL_TABLE_TEGA_FATURAMENTO = "faturamento"
MYSQL_TABLE_ANYMARKET = "anymarket_pedidos"
MYSQL_TABLE_TEGA_LOTE_ABERTO = "lotes_abertos"
MYSQL_TABLE_TEGA_PRODUCAO = "producao"
MYSQL_TABLE_TEGA_NF = "tega_nf"

REQUEST_TIMEOUT = 30
PROCESS_MAX_WAIT = 2700  

# =========================================================
# FUNÇÕES MYSQL
# =========================================================

def conectar_mysql(database=None):
    conn_args = {
        "host": MYSQL_HOST,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD
    }
    if database:
        conn_args["database"] = database
    return mysql.connector.connect(**conn_args)

def criar_banco_mysql():
    conn = conectar_mysql()
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}")
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Banco {MYSQL_DB} criado ou já existia")

def criar_tabela_tega():
    conn = conectar_mysql(MYSQL_DB)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {MYSQL_TABLE_TEGA_FATURAMENTO} (
            Ano VARCHAR(4),
            Mes VARCHAR(2),
            EmpresaId INT,
            EmpresaNome VARCHAR(255),
            TotalNF INT,
            ValorContabil DECIMAL(15,2),
            ValorProdutos DECIMAL(15,2)
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Tabela {MYSQL_TABLE_TEGA_FATURAMENTO} criada ou já existia")

def criar_tabela_tega_lote():
    conn = conectar_mysql(MYSQL_DB)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {MYSQL_TABLE_TEGA_LOTE_ABERTO} (
            LoteProducaoNroLoteCliente INT,
            LoteProducaoEmpresaNome VARCHAR(255),
            LoteProducaoDtCadastro DATE,
            LoteProducaoDtProgramado DATE,
            LoteProducaoDtLote DATE,
            LoteProducaoFinalizado INT
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Tabela {MYSQL_TABLE_TEGA_LOTE_ABERTO} criada ou já existia")

def criar_tabela_tega_producao():
    conn = conectar_mysql(MYSQL_DB)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {MYSQL_TABLE_TEGA_PRODUCAO} (
            ID_UNICO VARCHAR(255) PRIMARY KEY,
            AcompLoteNroLote INT,
            AcompLoteNroNf INT,
            AcompLoteNfConcluida VARCHAR(255),
            AcompLoteEtiqueta VARCHAR(255),
            AcompLoteDescLoja VARCHAR(255),
            AcompLoteProdutoDescricao VARCHAR(255),
            AcompLoteReferencia VARCHAR(255),
            AcompLoteSetorDescricao VARCHAR(255),
            AcompLoteLida VARCHAR(255),
            AcompLoteQtdeEsperada INT,
            AcompLoteQtdeLida INT,
            AcompLoteTransferida INT,
            AcompLoteDiasUteisProducao INT,
            AcompLoteDiasUteisTotal INT,
            DataCaptura DATETIME
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Tabela {MYSQL_TABLE_TEGA_PRODUCAO} criada ou já existia")

def criar_tabela_tega_nf():
    conn = conectar_mysql(MYSQL_DB)
    cursor = conn.cursor()
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {MYSQL_TABLE_TEGA_NF} (
        NFEmpresaNomeInterno VARCHAR(200),
        NFCancelada INT,
        ItemNFProdutoDescricao VARCHAR(100),
        ItemNFProdutoQtdeVolume INT,
        ItemNFQtde INT,
        ItemNFReferenciaDescricao VARCHAR(100),
        ItemNFVlrContabil DECIMAL(18,2),
        ItemNFVlrFreteTotal DECIMAL(18,2),
        ItemNFVlrTotal DECIMAL(18,2),
        NFDtEmissao DATE,
        NFDtExpedicao DATE,
        NFEntidadeUF VARCHAR(10),
        NFNroPedidoCliente VARCHAR(100) NOT NULL,
        NFQtdeVolumes INT,
        NFTransportadoraNome VARCHAR(100),
        NFVlrContabil DECIMAL(18,2),
        NFVlrFrete DECIMAL(18,2),
        NFIdNFE VARCHAR(50),
        ID INT AUTO_INCREMENT PRIMARY KEY
    )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Tabela {MYSQL_TABLE_TEGA_NF} criada ou já existia")

def criar_tabela_anymarket():
    conn = conectar_mysql(MYSQL_DB)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {MYSQL_TABLE_ANYMARKET} (
            EmpresaNome VARCHAR(255),
            PedidoId VARCHAR(50),
            Título VARCHAR(255),
            PreçoItem DECIMAL(15,2),
            marketPlaceId VARCHAR(100),
            Status VARCHAR(50),
            Marketplace VARCHAR(100),
            ValorTotal DECIMAL(15,2),
            ValorFaturado DECIMAL(15,2),
            Quantidade INT,
            SkuPrincipal VARCHAR(20),
            EanPrincipal VARCHAR(100),
            DataCriacao DATETIME,
            Transportadora VARCHAR(255),
            DeliveredDate DATETIME,
            EstimateDate DATETIME,
            ShippedDate DATETIME,
            EstadoDestino VARCHAR(255),
            mshops BOOLEAN,
            DataRegistro DATETIME
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Tabela {MYSQL_TABLE_ANYMARKET} criada ou já existia")

def limpar_tabela(nome_tabela):
    conn = conectar_mysql(MYSQL_DB)
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {nome_tabela}")
        conn.commit()
        print(f"{datetime.now()} TRUNCATE {nome_tabela} feito com sucesso")
    except mysql.connector.Error:
        try:
            cursor.execute(f"DELETE FROM {nome_tabela}")
            conn.commit()
            print(f"{datetime.now()} DELETE {nome_tabela} feito")
        except Exception as e:
            print(f"{datetime.now()} Erro ao limpar tabela {nome_tabela}: {e}")
            conn.rollback()
    finally:
        cursor.close()
        conn.close()

# =========================================================
# RENOVAÇÃO DE TOKEN
# =========================================================

token = None
expiration = None
TOKEN_REFRESH_MARGIN = timedelta(minutes=5)

def get_headers():
    global token, expiration

    agora = datetime.now(timezone.utc)

    if token is None or expiration is None or agora >= (expiration - timedelta(minutes=10)):
        resp = requests.post(
            URL_TEGA_AUTH,
            headers={
                "x-saas-licenca-id": X_SAAS_LICENCA_ID,
                "x-api-key": X_API_KEY
            },
            timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()

        data = resp.json()
        raw_exp = data["expirationDate"]
        exp_dt = datetime.fromisoformat(raw_exp)
        expiration = exp_dt.astimezone(timezone.utc)
        token = data["token"]

        print(f"Novo token gerado. Expira em (UTC): {expiration}")

    return {
        "Authorization": f"Bearer {token}",
        "x-saas-licenca-id": X_SAAS_LICENCA_ID
    }

def safe_request_get(url, params=None, headers=None):
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)

        if resp.status_code == 429:
            return resp
        resp.raise_for_status()
        return resp
    except requests.exceptions.RequestException as e:
        raise

# =========================================================
# FUNÇÕES FATURAMENTO
# =========================================================

def buscar_dados_tega():
    resp = safe_request_get(URL_TEGA_FATURAMENTO, headers=get_headers())
    if resp is None:
        return []
    dados = resp.json()
    return dados.get("faturamentoano", [])

def inserir_dados_tega(dados):
    limpar_tabela(MYSQL_TABLE_TEGA_FATURAMENTO)
    if not dados:
        print(f"{datetime.now()}Nenhum dado de faturamento para inserir")
        return

    conn = conectar_mysql(MYSQL_DB)
    cursor = conn.cursor()
    for item in dados:
        cursor.execute(f"""
            INSERT INTO {MYSQL_TABLE_TEGA_FATURAMENTO} (Ano, Mes, EmpresaId, EmpresaNome, TotalNF, ValorContabil, ValorProdutos)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            str(item.get("Ano")),
            str(item.get("Mes")).zfill(2),
            item.get("EmpresaId"),
            item.get("EmpresaNome"),
            int(item.get("TotalNF", 0)),
            Decimal(str(item.get("ValorContabil", "0"))),
            Decimal(str(item.get("ValorProdutos", "0")))
        ))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"{datetime.now()} Dados de Faturamento inseridos")

# =========================================================
# FUNÇÕES LOTES
# =========================================================

def buscar_dados_tega_lotes():
    pagina = 1
    limit = 1000
    todos_lotes = []

    while True:
        try:
            resp = safe_request_get(
                URL_TEGA_LOTE_ABERTO,
                headers=get_headers(),
                params={
                    "Pagenumber": pagina,
                    "Limitperpage": limit
                }
            )

            if resp is None:
                time.sleep(5)
                continue

            if resp.status_code == 429:
                print(f"429 Too Many Requests - aguardando 20s...")
                time.sleep(20)
                continue

            dados = resp.json()
            lotes = dados.get("lotesemaberto", [])
            total_pages = int(dados.get("TotalPages", 1) or 1)

            todos_lotes.extend(lotes)

            print(f"{datetime.now()} Página {pagina} retornou {len(lotes)} lotes (Total acumulado: {len(todos_lotes)})")

            if not lotes or pagina >= total_pages:
                print(f"Finalizado: {len(todos_lotes)} lotes coletados de {total_pages} páginas")
                break

            pagina += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"Erro na requisição da página {pagina}: {e}")
            time.sleep(5)
            continue

    return todos_lotes

def inserir_dados_tega_lotes(dados_lotes):
    limpar_tabela(MYSQL_TABLE_TEGA_LOTE_ABERTO)
    if not dados_lotes:
        print(f"{datetime.now()} Nenhum lote para inserir")
        return

    conn = conectar_mysql(MYSQL_DB)
    cursor = conn.cursor()

    chunk_size = 1000
    for i in range(0, len(dados_lotes), chunk_size):
        bloco = dados_lotes[i:i+chunk_size]
        registros_para_inserir = []

        for item in bloco:
            try:
                dt_cadastro = None
                dt_programado = None
                dt_lote = None
                try:
                    if item.get("LoteProducaoDtCadastro"):
                        dt_cadastro = datetime.fromisoformat(item.get("LoteProducaoDtCadastro")).date()
                except Exception:
                    dt_cadastro = None
                try:
                    if item.get("LoteProducaoDtProgramado"):
                        dt_programado = datetime.fromisoformat(item.get("LoteProducaoDtProgramado")).date()
                except Exception:
                    dt_programado = None
                try:
                    if item.get("LoteProducaoDtLote"):
                        dt_lote = datetime.fromisoformat(item.get("LoteProducaoDtLote")).date()
                except Exception:
                    dt_lote = None

                registros_para_inserir.append((
                    item.get("LoteProducaoNroLoteCliente"),
                    item.get("LoteProducaoEmpresaNome"),
                    dt_cadastro,
                    dt_programado,
                    dt_lote,
                    item.get("LoteProducaoFinalizado")
                ))
            except Exception as e:
                print(f"Erro ao processar lote {item.get('LoteProducaoNroLoteCliente')}: {e}")
                continue

        try:
            cursor.executemany(f"""
                INSERT INTO {MYSQL_TABLE_TEGA_LOTE_ABERTO} 
                (LoteProducaoNroLoteCliente, LoteProducaoEmpresaNome, LoteProducaoDtCadastro, LoteProducaoDtProgramado, LoteProducaoDtLote, LoteProducaoFinalizado)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, registros_para_inserir)
            conn.commit()
            print(f"{datetime.now()} {len(registros_para_inserir)} lotes inseridos no MySQL (bloco {i//chunk_size + 1})")
        except mysql.connector.Error as err:
            print(f"{datetime.now()} Erro ao inserir bloco de lotes: {err}")
            conn.rollback()

    cursor.close()
    conn.close()

# =========================================================
# FUNÇÕES PRODUÇÃO
# =========================================================

def buscar_dados_tega_producao():
    pagina = 1
    limit = 500
    todos_producao = []
    data_final = datetime.now().date()
    data_inicial = data_final

    print(f"{datetime.now()} Coletando produção de {data_inicial}")

    while True:
        try:
            resp = safe_request_get(
                URL_TEGA_PRODUCAO,
                headers=get_headers(),
                params={
                    "Pagenumber": pagina,
                    "Limitperpage": limit,
                    "Inicialdt": data_inicial.isoformat(),
                    "Finaldt": data_final.isoformat()
                }
            )

            if resp is None:
                time.sleep(5)
                continue

            # Se API respondeu vazio, fim imediato
            if not resp.text or resp.text.strip() == "":
                print(f"API de produção retornou vazio na página {pagina}. Finalizando coleta.")
                break

            if resp.status_code == 429:
                print("429 Too Many Requests - aguardando 20s...")
                time.sleep(20)
                continue

            try:
                dados = resp.json()
            except Exception:
                print(f"API retornou conteúdo não-JSON na página {pagina}. Encerrando coleta.")
                break

            producao = dados.get("producao", [])
            if not producao:
                print(f"Nenhum item de produção na página {pagina}. Encerrando coleta.")
                break

            total_records = int(dados.get("TotalRecords", 0) or 0)
            total_pages = int(dados.get("TotalPages", 1) or 1)

            for item in producao:
                if not item.get("AcompLoteNroLote"):
                    continue
                etiqueta = item.get("AcompLoteEtiqueta", "").strip()
                setor = item.get("AcompLoteSetorDescricao", "").strip()
                item["ID_UNICO"] = f"{etiqueta}_{setor}" if etiqueta and setor else None
                item["DataCaptura"] = datetime.now()

            todos_producao.extend([p for p in producao if p.get("ID_UNICO")])

            print(f"{datetime.now()} Página {pagina} retornou {len(producao)} registros (Total acumulado: {len(todos_producao)}/{total_records})")

            if pagina >= total_pages:
                print(f"Finalizado: {len(todos_producao)} registros coletados de {total_records}")
                break

            pagina += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"Erro na requisição da página {pagina}: {e}")
            time.sleep(5)
            continue

    return todos_producao

def inserir_dados_tega_producao(dados_producao):
    if not dados_producao:
        print(f"{datetime.now()} ⚠️ Nenhum dado de produção para inserir")
        return

    conn = conectar_mysql(MYSQL_DB)
    cursor = conn.cursor()
    chunk_size = 1000
    total_inseridos = 0

    sql = f"""
        INSERT INTO {MYSQL_TABLE_TEGA_PRODUCAO}
        (ID_UNICO, AcompLoteNroLote, AcompLoteNroNf, AcompLoteNfConcluida, AcompLoteEtiqueta,
         AcompLoteDescLoja, AcompLoteProdutoDescricao, AcompLoteReferencia, AcompLoteSetorDescricao,
         AcompLoteLida, AcompLoteQtdeEsperada, AcompLoteQtdeLida, AcompLoteTransferida,
         AcompLoteDiasUteisProducao, AcompLoteDiasUteisTotal, DataCaptura)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            AcompLoteNroLote = VALUES(AcompLoteNroLote),
            AcompLoteNroNf = VALUES(AcompLoteNroNf),
            AcompLoteNfConcluida = VALUES(AcompLoteNfConcluida),
            AcompLoteDescLoja = VALUES(AcompLoteDescLoja),
            AcompLoteProdutoDescricao = VALUES(AcompLoteProdutoDescricao),
            AcompLoteReferencia = VALUES(AcompLoteReferencia),
            AcompLoteLida = VALUES(AcompLoteLida),
            AcompLoteQtdeEsperada = VALUES(AcompLoteQtdeEsperada),
            AcompLoteQtdeLida = VALUES(AcompLoteQtdeLida),
            AcompLoteTransferida = VALUES(AcompLoteTransferida),
            AcompLoteDiasUteisProducao = VALUES(AcompLoteDiasUteisProducao),
            AcompLoteDiasUteisTotal = VALUES(AcompLoteDiasUteisTotal),
            DataCaptura = VALUES(DataCaptura)
    """

    for i in range(0, len(dados_producao), chunk_size):
        bloco = dados_producao[i:i + chunk_size]
        registros = []
        for item in bloco:
            registros.append((
                item.get("ID_UNICO"),
                item.get("AcompLoteNroLote"),
                item.get("AcompLoteNroNf"),
                item.get("AcompLoteNfConcluida"),
                item.get("AcompLoteEtiqueta"),
                item.get("AcompLoteDescLoja"),
                item.get("AcompLoteProdutoDescricao"),
                item.get("AcompLoteReferencia"),
                item.get("AcompLoteSetorDescricao"),
                item.get("AcompLoteLida"),
                item.get("AcompLoteQtdeEsperada"),
                item.get("AcompLoteQtdeLida"),
                item.get("AcompLoteTransferida"),
                item.get("AcompLoteDiasUteisProducao"),
                item.get("AcompLoteDiasUteisTotal"),
                item.get("DataCaptura")
            ))

        try:
            cursor.executemany(sql, registros)
            conn.commit()
            total_inseridos += len(registros)
            print(f"{datetime.now()} {len(registros)} registros processados (bloco {i//chunk_size + 1}) | Total: {total_inseridos}")
        except mysql.connector.Error as err:
            print(f"{datetime.now()} Erro ao inserir bloco: {err}")
            conn.rollback()

    cursor.close()
    conn.close()

# =========================================================
# FUNÇÕES NOTAS FISCAIS
# =========================================================

def buscar_dados_tega_nf():
    pagina = 1
    todas_nf = []

    while True:
        try:
            resp = safe_request_get(
                URL_TEGA_NF,
                headers=get_headers(),
                params={
                    "Pagenumber": pagina,
                    "Inicialdt": (datetime.now().date() - timedelta(days=60)).isoformat(),
                    "Finaldt": datetime.now().date().isoformat()
                }
            )

            if resp is None:
                time.sleep(5)
                continue

            if resp.status_code == 429:
                print("429 Too Many Requests - aguardando 20s...")
                time.sleep(20)
                continue

            dados = resp.json()
            notas = dados.get("NotasFiscais", [])
            total_pages = int(dados.get("TotalPages", 1) or 1)
            total_records = int(dados.get("TotalRecords", 0) or 0)

            todas_nf.extend(notas)

            print(f"{datetime.now()} Página {pagina} retornou {len(notas)} NFs (Total acumulado: {len(todas_nf)}/{total_records})")

            if not notas or pagina >= total_pages:
                print(f"Finalizado: {len(todas_nf)} notas coletadas de {total_pages} páginas")
                break

            pagina += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"Erro na requisição da página {pagina}: {e}")
            time.sleep(5)
            continue

    return todas_nf

def inserir_dados_tega_nf(dados_nf):
    limpar_tabela(MYSQL_TABLE_TEGA_NF)
    if not dados_nf:
        print(f"{datetime.now()} Nenhuma NF para inserir")
        return

    conn = conectar_mysql(MYSQL_DB)
    cursor = conn.cursor()

    def safe_date(value):
        try:
            if value and value != "0000-00-00":
                return datetime.fromisoformat(value).date()
        except Exception:
            pass
        return None

    chunk_size = 1000
    total_inseridos = 0

    for i in range(0, len(dados_nf), chunk_size):
        bloco = dados_nf[i:i+chunk_size]
        registros_para_inserir = []

        for nf in bloco:
            nf_id = nf.get("NFNroPedidoCliente")
            if not nf_id:
                continue

            for item in nf.get("Item", []):
                try:
                    dt_emissao = safe_date(nf.get("NFDtEmissao"))
                    dt_expedicao = safe_date(nf.get("NFDtExpedicao"))

                    registros_para_inserir.append((
                        nf.get("NFEmpresaNomeInterno"),
                        nf.get("NFCancelada"),
                        item.get("ItemNFProdutoDescricao"),
                        item.get("ItemNFProdutoQtdeVolume"),
                        item.get("ItemNFQtde"),
                        item.get("ItemNFReferenciaDescricao"),
                        item.get("ItemNFVlrContabil"),
                        item.get("ItemNFVlrFreteTotal"),
                        item.get("ItemNFVlrTotal"),
                        dt_emissao,
                        dt_expedicao,
                        nf.get("NFEntidadeUF"),
                        nf_id,
                        nf.get("NFQtdeVolumes"),
                        nf.get("NFTransportadoraNome"),
                        nf.get("NFVlrContabil"),
                        nf.get("NFVlrFrete"),
                        nf.get("NFIdNFE")
                    ))
                except Exception as e:
                    print(f"Erro ao processar NF {nf_id}: {e}")
                    continue

        if registros_para_inserir:
            try:
                cursor.executemany(f"""
                    INSERT INTO {MYSQL_TABLE_TEGA_NF}
                    (NFEmpresaNomeInterno, NFCancelada, ItemNFProdutoDescricao, ItemNFProdutoQtdeVolume, ItemNFQtde,
                     ItemNFReferenciaDescricao, ItemNFVlrContabil, ItemNFVlrFreteTotal,
                     ItemNFVlrTotal, NFDtEmissao, NFDtExpedicao, NFEntidadeUF,
                     NFNroPedidoCliente, NFQtdeVolumes, NFTransportadoraNome,
                     NFVlrContabil, NFVlrFrete, NFIdNFE)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, registros_para_inserir)
                conn.commit()
                total_inseridos += len(registros_para_inserir)
                print(f"{datetime.now()} {len(registros_para_inserir)} NFs inseridas no MySQL (bloco {i//chunk_size + 1}) | Total acumulado: {total_inseridos}")
            except mysql.connector.Error as err:
                print(f"{datetime.now()} Erro ao inserir bloco de NFs: {err}")
                conn.rollback()

    cursor.close()
    conn.close()

# =========================================================
# FUNÇÕES ANYMARKET
# =========================================================

def coletar_e_inserir_anymarket():
    pedidos_totais = []

    for empresa_id, empresa in ANYMARKET_CONFIG['empresas'].items():
        gumga_token = empresa['gumga_token']
        nome_empresa = empresa['nome']
        pedidos_empresa_count = 0

        print(f"{datetime.now()} 🔹 Coletando pedidos AnyMarket - {nome_empresa}...")

        limit = 100
        offset = 0
        url_base = f"{ANYMARKET_CONFIG['base_url']}/orders"

        while True:
            headers = {'gumgaToken': gumga_token, 'Content-Type': 'application/json'}
            params = {'limit': limit, 'offset': offset}

            max_tentativas = 5
            tentativa = 0
            sucesso = False

            # Tratamento de erros
            while tentativa < max_tentativas and not sucesso:
                try:
                    resp = requests.get(url_base, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
                    print(f"{datetime.now()} Status Code: {resp.status_code} | Offset: {offset}")
                    resp.raise_for_status()
                    dados = resp.json()
                    pedidos_pagina = dados.get('content', [])
                    sucesso = True

                except requests.exceptions.HTTPError as e:
                    if resp.status_code == 429:
                        tentativa += 1
                        print(f"{datetime.now()} 429 recebido - aguardar 20s (tentativa {tentativa})")
                        time.sleep(20)
                        continue
                    if 500 <= resp.status_code < 600:
                        tentativa += 1
                        print(f"{datetime.now()} Erro {resp.status_code} no offset {offset}, tentativa {tentativa}/{max_tentativas}, aguardando 10s...")
                        time.sleep(10)
                        continue
                    else:
                        print(f"{datetime.now()} HTTP Error {resp.status_code} para {nome_empresa}: {e}")
                        break
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                    tentativa += 1
                    print(f"{datetime.now()} Erro temporário no offset {offset}, tentativa {tentativa}/{max_tentativas}: {e}")
                    time.sleep(10)
                except requests.exceptions.RequestException as e:
                    print(f"{datetime.now()} Erro de requisição inesperado: {e}")
                    break

            if not sucesso:
                print(f"{datetime.now()} Falha ao buscar pedidos no offset {offset} após {max_tentativas} tentativas. Pulando para o próximo lote.")
                offset += limit
                continue

            # Processamento de página geral, tratamento de dados
            for p in pedidos_pagina:
                itens = p.get('items', [])
                qtd_total = 0
                sku_principal = ""
                ean_principal = ""
                title = ""
                preco_item = ""

                if itens and isinstance(itens[0], dict):
                    qtd_total = int(itens[0].get('amount', 0))
                    sku_principal = str(itens[0].get('sku', {}).get('partnerId', "")) if isinstance(itens[0].get('sku'), dict) else str(itens[0].get('sku', ""))
                    ean_principal = str(itens[0].get('sku', {}).get('ean', "")) if isinstance(itens[0].get('sku'), dict) else ""
                    title = str(itens[0].get('sku', {}).get('title', "")) if isinstance(itens[0].get('sku'), dict) else ""
                    preco_item = str(itens[0].get('total', 0))

                valor_total = p.get('gross', 0)
                if not isinstance(valor_total, (int, float, str)):
                    valor_total = 0
                valor_total = Decimal(str(valor_total))

                valor_faturado = p.get('total', 0)
                if not isinstance(valor_faturado, (int, float, str)):
                    valor_faturado = 0
                valor_faturado = Decimal(str(valor_faturado))

                def parse_datetime_iso(value):
                    if not value:
                        return None
                    try:
                        return datetime.fromisoformat(value.replace("Z", "+00:00"))
                    except Exception:
                        return None

                created_at = p.get("createdAt")
                data_criacao_datetime = None
                if created_at:
                    try:
                        if isinstance(created_at, (int, float, str)) and str(created_at).isdigit():
                            ts = int(created_at) / 1000
                            data_criacao_datetime = datetime.fromtimestamp(ts)
                        else:
                            data_criacao_datetime = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    except Exception as e:
                        print(f"Erro ao converter DataCriacao: {created_at} | {e}")
                        data_criacao_datetime = None

                tracking = p.get("tracking", {})
                delivered_date = parse_datetime_iso(tracking.get("deliveredDate"))
                estimate_date = parse_datetime_iso(tracking.get("estimateDate"))
                shipped_date = parse_datetime_iso(tracking.get("shippedDate"))

                shippingCarrierNormalized = ""
                if itens and isinstance(itens[0], dict):
                    shippings = itens[0].get("shippings", [])
                    if shippings and isinstance(shippings[0], dict):
                        shippingCarrierNormalized = shippings[0].get("shippingCarrierNormalized", "")

                estado_destino = ""
                shipping_info = p.get("shipping", {})
                if isinstance(shipping_info, dict):
                    estado_destino = shipping_info.get("stateNameNormalized", "")

                metadata = p.get("metadata", {})
                mshops_str = metadata.get("mshops", "")

                if isinstance(mshops_str, str):
                    mshops_str = mshops_str.strip().lower()
                    if mshops_str == "true":
                        mshops = True
                    elif mshops_str == "false":
                        mshops = False
                    else:
                        mshops = None
                else:
                    mshops = None

                mp_id = p.get("marketPlaceId") or p.get("marketplaceId") or ""

                pedidos_totais.append({
                    "EmpresaNome": str(nome_empresa),
                    "PedidoId": str(p.get("id", "")),
                    "Título": title,
                    "PreçoItem": preco_item,
                    "marketPlaceId": str(mp_id),
                    "Status": str(p.get("status", "")),
                    "Marketplace": str(p.get("marketPlace", "")),
                    "ValorTotal": valor_total,
                    "ValorFaturado": valor_faturado,
                    "Quantidade": qtd_total,
                    "SkuPrincipal": sku_principal,
                    "EanPrincipal": ean_principal,
                    "DataCriacao": data_criacao_datetime,
                    "Transportadora": shippingCarrierNormalized,
                    "DeliveredDate": delivered_date,
                    "EstimateDate": estimate_date,
                    "ShippedDate": shipped_date,
                    "EstadoDestino": estado_destino,
                    "mshops": mshops,
                    "DataRegistro": datetime.now()
                })

            pedidos_empresa_count += len(pedidos_pagina)
            if len(pedidos_pagina) < limit:
                break

            offset += limit

        print(f"{datetime.now()} Coleta para '{nome_empresa}' finalizada. Total de pedidos: {pedidos_empresa_count}")

    print(f"{datetime.now()} Total geral de pedidos coletados: {len(pedidos_totais)}")

    if not pedidos_totais:
        print(f"{datetime.now()} Nenhum pedido para inserir.")
        return

    limpar_tabela(MYSQL_TABLE_ANYMARKET)
    conn = conectar_mysql(MYSQL_DB)
    cursor = conn.cursor()
    query = f"""
        INSERT INTO {MYSQL_TABLE_ANYMARKET} 
        (EmpresaNome, PedidoId, Título, PreçoItem, marketPlaceId, Status, Marketplace, ValorTotal, ValorFaturado, Quantidade, SkuPrincipal, EanPrincipal, DataCriacao,
         Transportadora, DeliveredDate, EstimateDate, ShippedDate, EstadoDestino, mshops, DataRegistro)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    dados_para_inserir = [
        (
            item["EmpresaNome"],
            item["PedidoId"],
            item["Título"],
            item["PreçoItem"],
            item["marketPlaceId"],
            item["Status"],
            item["Marketplace"],
            item["ValorTotal"],
            item["ValorFaturado"],
            item["Quantidade"],
            item["SkuPrincipal"],
            item["EanPrincipal"],
            item["DataCriacao"],
            item["Transportadora"],
            item["DeliveredDate"],
            item["EstimateDate"],
            item["ShippedDate"],
            item["EstadoDestino"],
            item["mshops"],
            item["DataRegistro"]
        ) for item in pedidos_totais
    ]

    try:
        cursor.executemany(query, dados_para_inserir)
        conn.commit()
        print(f"{datetime.now()} 🔹 ✅ {cursor.rowcount} registros da AnyMarket inseridos com sucesso.")
    except mysql.connector.Error as err:
        print(f"{datetime.now()} 🔹 ❌ Erro ao inserir dados da AnyMarket: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# =========================================================
# MULTIPROCESSOS
# =========================================================

def atualizar_tega_completo():
    try:
        print(f"{datetime.now()} Atualizando Faturamento Tega...")
        dados = buscar_dados_tega()
        inserir_dados_tega(dados)
    except Exception as e:
        print(f"Erro no faturamento Tega: {e}\n{traceback.format_exc()}")

    try:
        print(f"{datetime.now()} Atualizando Lotes Tega...")
        lotes = buscar_dados_tega_lotes()
        inserir_dados_tega_lotes(lotes)
    except Exception as e:
        print(f"Erro nos lotes Tega: {e}\n{traceback.format_exc()}")

    try:
        print(f"{datetime.now()} Atualizando Produção Tega...")
        producao = buscar_dados_tega_producao()
        inserir_dados_tega_producao(producao)
    except Exception as e:
        print(f"Erro na produção Tega: {e}\n{traceback.format_exc()}")

    try:
        print(f"{datetime.now()} Atualizando Notas Fiscais Tega...")
        notas = buscar_dados_tega_nf()
        inserir_dados_tega_nf(notas)
    except Exception as e:
        print(f"Erro nas NFs Tega: {e}\n{traceback.format_exc()}")

def atualizar_anymarket_process():
    try:
        print(f"{datetime.now()} Atualizando AnyMarket...")
        coletar_e_inserir_anymarket()
    except Exception as e:
        print(f"Erro AnyMarket: {e}\n{traceback.format_exc()}")

def atualizar_tega_completo_process():
    try:
        print(f"{datetime.now()} Iniciando Tega completo...")
        atualizar_tega_completo()
    except Exception as e:
        print(f"Erro no processo Tega: {e}\n{traceback.format_exc()}")

def atualizar_anymarket_process_wrapper():
    try:
        print(f"{datetime.now()} Iniciando AnyMarket...")
        atualizar_anymarket_process()
    except Exception as e:
        print(f"Erro no processo AnyMarket: {e}\n{traceback.format_exc()}")

# =========================================================
# FUNÇÃO PRINCIPAL
# =========================================================

def main():
    # cria banco e tabelas
    criar_banco_mysql()
    criar_tabela_tega()
    criar_tabela_tega_lote()
    criar_tabela_tega_producao()
    criar_tabela_anymarket()
    criar_tabela_tega_nf()

    # configura logging uma única vez
    log_path = r"C:\Users\path\log_automacao.txt"
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s - %(message)s'
    )

    intervalo_segundos = 60 * 60

    while True:
        inicio_ciclo = datetime.now()
        print(f"{inicio_ciclo} Iniciando atualização de Tega e AnyMarket...")
        logging.info("Iniciando atualização de Tega e AnyMarket...")

        # cria processos
        p_tega = Process(target=atualizar_tega_completo_process)
        p_anymarket = Process(target=atualizar_anymarket_process_wrapper)

        # inicia processos
        p_tega.start()
        p_anymarket.start()

        # aguarda ambos finalizarem com timeout para evitar bloqueios indefinidos
        p_tega.join(PROCESS_MAX_WAIT)
        if p_tega.is_alive():
            print(f"{datetime.now()} Processo Tega excedeu {PROCESS_MAX_WAIT}s. Finalizando forçadamente...")
            try:
                p_tega.terminate()
            except Exception as e:
                print(f"Erro ao terminar processo Tega: {e}")

        p_anymarket.join(PROCESS_MAX_WAIT)
        if p_anymarket.is_alive():
            print(f"{datetime.now()} Processo AnyMarket excedeu {PROCESS_MAX_WAIT}s. Finalizando forçadamente...")
            try:
                p_anymarket.terminate()
            except Exception as e:
                print(f"Erro ao terminar processo AnyMarket: {e}")

        fim_ciclo = datetime.now()
        tempo_execucao = (fim_ciclo - inicio_ciclo).total_seconds()
        tempo_restante = intervalo_segundos - tempo_execucao

        if tempo_restante > 0:
            print(f"{fim_ciclo} Atualização concluída. Aguardando {tempo_restante/60:.1f} minutos até a próxima rodada...")
            logging.info(f"Atualização concluída. Próxima rodada em {tempo_restante/60:.1f} minutos.")
            time.sleep(tempo_restante)
        else:
            print(f"{fim_ciclo} Execução demorou mais que o intervalo. Reiniciando imediatamente...")
            logging.warning("Execução demorou mais que o intervalo. Reiniciando imediatamente...")

        # log periódico por ciclo bem sucedido
        logging.info("Automação rodando normalmente.")

if __name__ == "__main__":
    main()
