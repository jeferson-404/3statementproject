-- ============================================================
-- CAMADA BRONZE — Dados brutos que vem da CVM 
-- 
-- Incremental: dado o volume pequeno (ZIPs de 8-13 MB, ~32 arquivos
--              no total) e a frequência baixa de execução (trimestral),
--              o script SEMPRE baixa todos os ZIPs e decide se precisa
--              reprocessar comparando o hashdo conteúdo com o
--              último hash gravado em `controle_ingestao` para aquele
--              (tipo_documento, ano_documento). Se o hash for igual,
--              pula o processamento; se for diferente (ou a primeira vez que aparece),
--              processa e faz upsert..
-- Upsert     : a chave natural n inclui `versao`. Quando algum documento é atualizado 
--              a linha existente é sobrescrita com o
--              valor mais recente — a bronze sempre reflete "o número
--              certo agora". As colunas `versao` e `data_carga` ficam
--              guardadas, para falar qual versão está ali e quando foi atualizada
--              pela última vez..
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bronze;


-- ------------------------------------------------------------
-- 1) CONTROLE Da INGESTÃO 
-- ------------------------------------------------------------
-- O script baixa o ZIP de cada (tipo_documento, ano_documento) a cada
-- execução e calcula o hash sha256 do conteúdo. Antes de processar,
-- compara com o hash da última linha de status = 'SUCESSO' para aquele
-- mesmo (tipo_documento, ano_documento): se for igual, pula o
-- processamento mas se for diferente ou for a primeira vez,
-- processa e insere uma nova linha aqui. Log 100% append-only —
-- histórico completo de cada execução fica preservado para auditoria.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.controle_ingestao (
    id                   BIGSERIAL PRIMARY KEY,
    tipo_documento       VARCHAR(3)   NOT NULL
                         CHECK (tipo_documento IN ('DFP','ITR')),
    ano_documento        INTEGER      NOT NULL,
    nome_arquivo         VARCHAR(200) NOT NULL,
    url_origem           TEXT         NOT NULL,
    hash_arquivo         VARCHAR(64)  NOT NULL,   -- sha256 do conteúdo do ZIP
    data_download        TIMESTAMP    NOT NULL DEFAULT now(),
    data_processamento   TIMESTAMP,
    qtd_linhas_inseridas BIGINT       DEFAULT 0,
    status               VARCHAR(20)  NOT NULL DEFAULT 'PENDENTE'
                         CHECK (status IN ('PENDENTE','SUCESSO','ERRO')),
    mensagem_erro        TEXT,
    CONSTRAINT uq_controle_ingestao
        UNIQUE (tipo_documento, ano_documento, hash_arquivo)
);

COMMENT ON TABLE bronze.controle_ingestao IS
    'Log de cada ZIP anual baixado da CVM. O hash detecta '
    'se o conteúdo do arquivo mudou desde a última execução (ex: '
    'reapresentações no ano corrente), permitindo pular anos já '
    'processados sem alteração.';


-- ------------------------------------------------------------
-- 2) DEMONSTRATIVOS DE VALORES (BPA, BPP, DRE, DFC_MD, DFC_MI, DRA, DVA)
-- ------------------------------------------------------------
-- Essas 7 demonstrações compartilham a mesma estrutura de
-- colunas no layout da CVM, então ficam em uma única tabela genérica,
-- distinguidas pela coluna `demonstrativo`.
-- dt_ini_exerc é nula para BPA/BPP porque balanço é só a data de referência.
-- `versao` e `data_carga`  servem
-- só para saber qual versão do dado está gravada e quando entrou/foi
-- atualizada pela última vez. O loader faz UPSERT: se houver alterações
-- , a linha é sobrescrita, não duplicada.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.demonstrativos_valores (
    id                   BIGSERIAL PRIMARY KEY,
    tipo_documento       VARCHAR(3)    NOT NULL
                         CHECK (tipo_documento IN ('DFP','ITR')),
    demonstrativo        VARCHAR(10)   NOT NULL
                         CHECK (demonstrativo IN
                             ('BPA','BPP','DRE','DFC_MD','DFC_MI','DRA','DVA')),
    cnpj_cia             VARCHAR(20)   NOT NULL,
    denom_cia            VARCHAR(200),
    cd_cvm               INTEGER       NOT NULL,
    grupo_dfp            VARCHAR(100)  NOT NULL,  -- ex: 'DF Consolidado - Demonstração de Fluxo de Caixa (Método Indireto)'
    moeda                VARCHAR(10),
    escala_moeda         VARCHAR(10),              -- 'MIL' / 'UNIDADE'
    ordem_exerc          VARCHAR(15)   NOT NULL,   -- 'ÚLTIMO' / 'PENÚLTIMO'
    dt_refer             DATE          NOT NULL,
    versao               INTEGER       NOT NULL DEFAULT 1,   -- metadado: última versão recebida da CVM
    dt_ini_exerc         DATE,                      -- nulo para BPA/BPP
    dt_fim_exerc         DATE          NOT NULL,
    cd_conta             VARCHAR(20)   NOT NULL,
    ds_conta             VARCHAR(300),
    vl_conta             NUMERIC(30,4) NOT NULL,
    st_conta_fixa        CHAR(1),                   -- 'S' / 'N'
    ano_documento        INTEGER       NOT NULL,    -- ano do ZIP de origem
    nome_arquivo_origem  VARCHAR(200)  NOT NULL,
    data_carga           TIMESTAMP     NOT NULL DEFAULT now(),  -- quando essa linha foi inserida/atualizada pela última vez
    CONSTRAINT uq_demonstrativos_valores UNIQUE
        (tipo_documento, demonstrativo, cd_cvm, grupo_dfp,
         dt_refer, ordem_exerc, cd_conta)
);

CREATE INDEX IF NOT EXISTS idx_dv_cd_cvm        ON bronze.demonstrativos_valores (cd_cvm);
CREATE INDEX IF NOT EXISTS idx_dv_dt_refer      ON bronze.demonstrativos_valores (dt_refer);
CREATE INDEX IF NOT EXISTS idx_dv_demonstrativo ON bronze.demonstrativos_valores (demonstrativo);
CREATE INDEX IF NOT EXISTS idx_dv_cd_conta      ON bronze.demonstrativos_valores (cd_conta);

COMMENT ON TABLE bronze.demonstrativos_valores IS
    'Dados brutos, linha a linha, de BPA/BPP/DRE/DFC(MD/MI)/DRA/DVA '
    'exatamente como entregues pela CVM, sem transformação de negócio. '
    'Estratégia upsert: sempre reflete o valor mais recente por '
    '(empresa, demonstrativo, período, conta) — versao/data_carga são '
    'metadados, não fazem parte da chave. '
    'DMPL fica em tabela separada por ter uma dimensão extra (coluna_df).';


-- ------------------------------------------------------------
-- 3) DMPL 
-- ------------------------------------------------------------
-- Tem uma dimensão a mais: coluna_df (qual componente do PL a linha
-- se refere: Capital Social, Reservas de Lucro, Lucros Acumulados etc.)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.dmpl_valores (
    id                   BIGSERIAL PRIMARY KEY,
    tipo_documento       VARCHAR(3)    NOT NULL
                         CHECK (tipo_documento IN ('DFP','ITR')),
    cnpj_cia             VARCHAR(20)   NOT NULL,
    denom_cia            VARCHAR(200),
    cd_cvm               INTEGER       NOT NULL,
    grupo_dfp            VARCHAR(100)  NOT NULL,
    moeda                VARCHAR(10),
    escala_moeda         VARCHAR(10),
    ordem_exerc          VARCHAR(15)   NOT NULL,
    dt_refer             DATE          NOT NULL,
    versao               INTEGER       NOT NULL DEFAULT 1,   -- última versão recebida da CVM
    dt_ini_exerc         DATE,
    dt_fim_exerc         DATE          NOT NULL,
    cd_conta             VARCHAR(20)   NOT NULL,
    ds_conta             VARCHAR(300),
    coluna_df            VARCHAR(300)  NOT NULL,
    vl_conta             NUMERIC(30,4) NOT NULL,
    st_conta_fixa        CHAR(1),
    ano_documento        INTEGER       NOT NULL,
    nome_arquivo_origem  VARCHAR(200)  NOT NULL,
    data_carga           TIMESTAMP     NOT NULL DEFAULT now(),  -- quando essa linha foi inserida/atualizada pela última vez
    CONSTRAINT uq_dmpl_valores UNIQUE
        (tipo_documento, cd_cvm, grupo_dfp, dt_refer,
         ordem_exerc, cd_conta, coluna_df)
);

CREATE INDEX IF NOT EXISTS idx_dmpl_cd_cvm   ON bronze.dmpl_valores (cd_cvm);
CREATE INDEX IF NOT EXISTS idx_dmpl_dt_refer ON bronze.dmpl_valores (dt_refer);

COMMENT ON TABLE bronze.dmpl_valores IS
    'Dados brutos da Demonstração de Mutações do Patrimônio Líquido, '
    'separada das demais por ter a dimensão extra coluna_df. '
    'Estratégia upsert: versao/data_carga são metadados, não fazem '
    'parte da chave.';