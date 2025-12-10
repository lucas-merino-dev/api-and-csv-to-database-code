# 📊 Data Pipeline – APIs + CSV → MySQL → Power BI (WIP)

Este projeto tem como objetivo **capturar, processar e armazenar dados provenientes de múltiplas APIs e arquivos CSV**, inserindo-os em um banco **MySQL**, para posterior consumo e modelagem no **Power BI**.

O sistema automatiza a coleta, padronização e persistência dos dados, garantindo atualização consistente para análises e dashboards.

---

## 🚀 Funcionalidades

- 🔄 **Coleta automática** de dados em APIs de sistemas gerenciais.  
- 📁 **Leitura e tratamento de arquivos CSV**.  
- 🧹 Padronização de campos (datas, números, mensagens, status, chaves, IDs etc.).  
- 🗄️ Inserção estruturada em tabelas MySQL.  
- 🛡️ Tratamento de erros, logs e validação de dados.  
- 📡 Integração total com **Power BI** para relatórios e insights avançados.  

---
## 📖 Fluxo de Dados

1. **Coleta**  
   O script consulta APIs externas, utiliza tokens dinâmicos, headers personalizados e trata paginação e erros.
   
2. **Transformação**  
   Dados brutos passam por:
   - Normalização de campos  
   - Conversão de tipos  
   - Correção de formatação  
   - Validação de chaves  

3. **Carga (Load)**  
   Os dados são inseridos no MySQL usando operações otimizadas e controle de duplicidade.

4. **Visualização**  
   O Power BI consome as tabelas MySQL para criação de métricas, dashboards e análises.

---

## 🛠️ Tecnologias Utilizadas

- **Python 3**
  - `requests` – consumo de APIs  
  - `csv` / `pandas` – leitura e manipulação de CSV  
  - `mysql.connector` – conexão com MySQL  
  - `logging` – logs estruturados
  - `multiprocessing` - aceleração de fluxo por multiprocessamento

- **MySQL 5.7/8+**

- **Power BI Desktop / Power BI Service**

- **Agendador de Tarefas Windows**
