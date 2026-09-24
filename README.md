# Módulo 1: Camada Bronze 

Esta implementação do **Primeiro Módulo** da pipeline de dados financeiros do projeto. O objetivo principal deste módulo é realizar a extração, controle e carga (EL/ETL) automatizada das demonstrações financeiras publicadas pelas companhias abertas na **CVM** na **Camada Bronze** da nossa arquitetura Medallion.

## 🏗️ Arquitetura do Módulo

Guardar os dados brutos no banco de dados PostgreSQL, exatamente como fornecidos pela CVM.

```
    [ Fonte CVM ]
  (Arquivos .ZIP)
        │
        ▼
 ┌──────────────┐
 │ load_bronze  │ ──► [ Passos: Download em memória -> Hashing (SHA256) -> Parsing CSV -> Normalização ]
 └──────┬───────┘
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│               PostgreSQL (Schema: bronze)               │
├──────────────────────┬──────────────────────────────────┤
│ controle_ingestao    │ Audit Trail                       │
│ demonstrativos_valores│ BPA, BPP, DRE, DFC, DRA, DVA     │
│ dmpl_valores         │ DMPL (com dimensão extra)        │
└─────────────────────────────────────────────────────────┘

```

## 📁 Estrutura de Arquivos

* **`schema_bronze.sql`**: Script DDL SQL que cria o schema `bronze` e as tabelas operacionais e de negócio (`controle_ingestao`, `demonstrativos_valores` e `dmpl_valores`).

* **`bd_conect.py`**: Módul de infraestrutura para gerenciar e validar conexões com o banco de dados PostgreSQL via variáveis de ambiente, além de garantir a existência dos schemas da arquitetura Medallion (`bronze`, `silver`, `gold`).

* **`load_bronze.py`**: Script principal de orquestração do pipeline de extração e ingestão. Ele baixa os pacotes `.zip` da CVM diretamente na memória, verifica alterações de conteúdo, trata inconsistências de tipos/formatos e aplica estratégias de *upsert*.

## 🗄️ Estrutura do Banco de Dados (Schema `bronze`)

### 1. `bronze.controle_ingestao`

Armazena o histórico de todas as execuções, downloads efetuados e *hashes* de arquivos para controle de concorrência e evitar reprocessamento desnecessário.

* **Chave Única**: `(tipo_documento, ano_documento, hash_arquivo)`

* **Finalidade**: Identificar se o arquivo anual já foi processado anteriormente com sucesso.

### 2. `bronze.demonstrativos_valores`

Armazena as demonstrações contábeis genéricas que compartilham da mesma estrutura de colunas nos CSVs da CVM:

* **BPA**: Balanço Patrimonial Ativo

* **BPP**: Balanço Patrimonial Passivo

* **DRE**: Demonstração do Resultado do Exercício

* **DFC_MD / DFC_MI**: Demonstração do Fluxo de Caixa (Direto e Indireto)

* **DRA**: Demonstração do Resultado Abrangente

* **DVA**: Demonstração do Valor Adicionado

* **Chave Natural / Upsert**: `(tipo_documento, demonstrativo, cd_cvm, grupo_dfp, dt_refer, ordem_exerc, cd_conta)`

### 3. `bronze.dmpl_valores`

Isolada das demais por possuir uma dimensão adicional: a coluna **`coluna_df`** (referente ao componente do Patrimônio Líquido afetado, ex: *Capital Social*, *Reservas de Lucro*, etc.).

* **Demonstrativo**: **DMPL** (Demonstração das Mutações do Patrimônio Líquido).

* **Chave Natural / Upsert**: `(tipo_documento, cd_cvm, grupo_dfp, dt_refer, ordem_exerc, cd_conta, coluna_df)`

## ⚙️ Principais Regras 

1. **Deduplicação Inteligente por Hash (SHA256)**:
   Como os pacotes anuais da CVM contêm atualizações recorrentes (reapresentações), o script baixa os arquivos e calcula o hash SHA256 do arquivo em memória. Se o hash já consta como `SUCESSO` no log de controle, o processamento daquele ano é imediatamente saltado.

2. **Estratégia de Upsert Mutável e Versionado**:
   A camada Bronze sempre mantém o dado "correto de agora". Ao encontrar um documento atualizado ou reapresentado, o script atualiza o registro no banco (`ON CONFLICT DO UPDATE`) apenas se a versão da nova carga for **maior ou igual** à versão existente (`EXCLUDED.versao >= bronze.demonstrativos_valores.versao`).

3. **Processamento In-Memory**:
   Para evitar gargalos de I/O de disco, os arquivos `.zip`  são baixados e descompactados totalmente em memória

4. **Resiliência a Formatos de Entrada**:
   Tratamento explícito para leitura dos CSVs em codificação `latin1`, padronização das datas contábeis (tratando exceções de balanços fotográficos sem data inicial) e conversão do formato numérico científico/padrão fornecido pela CVM.

## 🚀 Como Executar este Módulo

### 1. Pré-requisitos

* Python 3.10 ou superior

* PostgreSQL rodando

* Dependências Python instaladas (`pandas`, `psycopg2-binary`, `requests`, `python-dotenv`)

### 2. Configuração do Ambiente (`.env`)

Crie um arquivo `.env` na raiz do projeto contendo as credenciais de acesso ao PostgreSQL:

```
PGHOST=localhost
PGPORT=5432
PGDATABASE=seu_banco
PGUSER=seu_usuario
PGPASSWORD=sua_senha

```

### 3. Criação da Estrutura DDL

Execute o arquivo DDL SQL no seu SGBD Postgres para inicializar as tabelas e índices do schema `bronze`:

```
psql -h localhost -U seu_usuario -d seu_banco -f schema_bronze.sql

```

### 4. Execução da Carga

Execute o script `load_bronze.py`:

```
python load_bronze.py

```