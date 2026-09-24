"""
Carga da camada Bronze — CVM (DFP/ITR) -> Postgres

Fluxo geral:
  1. Para cada (tipo_documento, ano), monta a URL e baixa o ZIP inteiro.
  2. Calcula o hash sha256 do conteúdo baixado.
  3. Consulta bronze.controle_ingestao: se esse hash já foi processado
     com SUCESSO para esse (tipo, ano), pula -- nada mudou.
  4. Caso contrário, extrai os CSVs de interesse do ZIP (BPA, BPP, DRE,
     DFC_MD, DFC_MI, DRA, DVA, DMPL), normaliza cada um e faz upsert
     nas tabelas bronze.demonstrativos_valores / bronze.dmpl_valores.
  5. Registra o resultado (sucesso ou erro) em bronze.controle_ingestao.

Ajuste ANO_INICIAL e ANO_FINAL antes de rodar.
"""

import hashlib
import io
import sys
import zipfile
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from bd_conect import get_connection

# ============================================================
# PASSO 0 — CONFIGURAÇÃO
# ============================================================
ANO_INICIAL = 2010
ANO_FINAL = 2026  # <-- ajuste para o ano corrente quando for rodar

TIPOS_DOCUMENTO = ["DFP", "ITR"]

URL_TEMPLATE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{tipo}/DADOS/{arquivo}"

# Demonstrações que compartilham a estrutura genérica (tabela demonstrativos_valores)
DEMONSTRATIVOS_GENERICOS = ["BPA", "BPP", "DRE", "DFC_MD", "DFC_MI", "DRA", "DVA"]
DEMONSTRATIVO_DMPL = "DMPL"  # tem estrutura própria (tabela dmpl_valores)

# Mapeamento das colunas do CSV da CVM (maiúsculas) para as colunas do banco (minúsculas)
MAPA_COLUNAS = {
    "CNPJ_CIA": "cnpj_cia",
    "DENOM_CIA": "denom_cia",
    "CD_CVM": "cd_cvm",
    "GRUPO_DFP": "grupo_dfp",
    "MOEDA": "moeda",
    "ESCALA_MOEDA": "escala_moeda",
    "ORDEM_EXERC": "ordem_exerc",
    "DT_REFER": "dt_refer",
    "VERSAO": "versao",
    "DT_INI_EXERC": "dt_ini_exerc",
    "DT_FIM_EXERC": "dt_fim_exerc",
    "CD_CONTA": "cd_conta",
    "DS_CONTA": "ds_conta",
    "VL_CONTA": "vl_conta",
    "ST_CONTA_FIXA": "st_conta_fixa",
    "COLUNA_DF": "coluna_df",
}


# ============================================================
# PASSO 1 — MONTAR A URL E BAIXAR O ZIP
# ============================================================
def montar_url(tipo_documento: str, ano: int):
    prefixo = tipo_documento.lower()
    nome_arquivo = f"{prefixo}_cia_aberta_{ano}.zip"
    url = URL_TEMPLATE.format(tipo=tipo_documento, arquivo=nome_arquivo)
    return url, nome_arquivo


def baixar_zip(url: str) -> bytes:
    """Baixa o ZIP inteiro para a memória (arquivos são pequenos: 8-13 MB cada)."""
    resposta = requests.get(url, timeout=120)
    resposta.raise_for_status()  # lança exceção se vier 404/500/etc.
    return resposta.content


# ============================================================
# PASSO 2 — CALCULAR O HASH DO CONTEÚDO BAIXADO
# ============================================================
def calcular_hash(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()


# ============================================================
# PASSO 3 — CONSULTAR SE ESSE HASH JÁ FOI PROCESSADO
# ============================================================
def hash_ja_processado(conn, tipo_documento: str, ano: int, hash_arquivo: str) -> bool:
    """
    True se já existe uma linha de status SUCESSO em controle_ingestao
    com exatamente esse (tipo_documento, ano_documento, hash_arquivo) --
    ou seja, já baixamos e carregamos esse conteúdo antes, nada mudou.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM bronze.controle_ingestao
            WHERE tipo_documento = %s AND ano_documento = %s
              AND hash_arquivo = %s AND status = 'SUCESSO'
            LIMIT 1
            """,
            (tipo_documento, ano, hash_arquivo),
        )
        return cur.fetchone() is not None


# ============================================================
# PASSO 4 — EXTRAIR OS CSVs DE INTERESSE DE DENTRO DO ZIP
# ============================================================
def extrair_csvs(conteudo_zip: bytes, tipo_documento: str, ano: int) -> dict:
    """
    Abre o ZIP direto da memória (sem gravar em disco) e devolve um
    dicionário {demonstrativo: DataFrame} só com as versões
    CONSOLIDADAS (sufixo _con_) de cada demonstrativo.
    """
    prefixo = tipo_documento.lower()
    demonstrativos = DEMONSTRATIVOS_GENERICOS + [DEMONSTRATIVO_DMPL]
    resultado = {}

    with zipfile.ZipFile(io.BytesIO(conteudo_zip)) as zf:
        nomes_no_zip = zf.namelist()
        for demo in demonstrativos:
            nome_csv = f"{prefixo}_cia_aberta_{demo}_con_{ano}.csv"
            if nome_csv not in nomes_no_zip:
                # Nem todo demonstrativo existe em todo ano (ex: DFC_MD é raro)
                continue
            with zf.open(nome_csv) as f:
                # decimal="," aqui NÃO teria efeito com dtype=str (o pandas
                # ignora o parâmetro quando lê tudo como texto) -- por isso
                # a conversão de vl_conta é feita manualmente no Passo 5,
                # em normalizar_dataframe, que sabe lidar com o formato
                # numérico brasileiro (ponto de milhar, vírgula decimal).
                df = pd.read_csv(
                    f,
                    sep=";",
                    encoding="latin1",  # padrão dos CSVs da CVM
                    dtype=str,  # lê tudo como texto; convertemos tipo a tipo no Passo 5
                )
            resultado[demo] = df
    return resultado


# ============================================================
# PASSO 5 — NORMALIZAR UM DATAFRAME PARA O FORMATO DA TABELA
# ============================================================
def verificar_colunas_mapeadas(df_bruto: pd.DataFrame, demonstrativo: str, ano: int) -> None:
    """
    Confere se todas as colunas que vieram no CSV real da CVM estão
    cobertas pelo MAPA_COLUNAS. Se aparecer alguma coluna nova (que a
    CVM pode adicionar em qualquer momento, sem aviso), isso é
    impresso como AVISO em vez de a coluna ser descartada em silêncio.
    Não é um erro fatal -- só um alerta para você investigar depois.
    """
    colunas_do_csv = set(df_bruto.columns)
    colunas_conhecidas = set(MAPA_COLUNAS.keys())
    desconhecidas = colunas_do_csv - colunas_conhecidas
    if desconhecidas:
        print(
            f"[AVISO] {demonstrativo} {ano}: colunas não mapeadas em MAPA_COLUNAS "
            f"e por isso ignoradas: {sorted(desconhecidas)}"
        )


# Todas as colunas que a tabela de destino espera receber. Diferente do
# nome antigo "colunas_possiveis", esta lista é uma GARANTIA: toda coluna
# aqui vai existir no DataFrame de saída, mesmo que a fonte não a traga
# (ex: BPA/BPP não têm DT_INI_EXERC) -- preenchida com None nesse caso.
# Sem essa garantia, upsert_demonstrativos/upsert_dmpl quebram com
# KeyError ao tentar selecionar uma coluna que nunca existiu.
COLUNAS_ESPERADAS = [
    "cnpj_cia",
    "denom_cia",
    "cd_cvm",
    "grupo_dfp",
    "moeda",
    "escala_moeda",
    "ordem_exerc",
    "dt_refer",
    "versao",
    "dt_ini_exerc",
    "dt_fim_exerc",
    "cd_conta",
    "ds_conta",
    "vl_conta",
    "st_conta_fixa",
    "coluna_df",
]


def _converter_valor_numerico(serie: pd.Series) -> pd.Series:
    """
    Converte a coluna VL_CONTA (texto) para float.

    O CSV real da CVM traz o valor em formato decimal padrão, com ponto
    e sem separador de milhar, ex: "4738841410.0000000000" (== 4,7
    bilhões). NÃO é o formato brasileiro "1.234,56" que se poderia supor
    à primeira vista -- confirmado inspecionando o CSV real de 2023
    (ver BUGS_load_bronze.md, Bug 4). Por isso pd.to_numeric funciona
    direto, sem nenhuma limpeza de string.
    """
    return pd.to_numeric(serie, errors="coerce")



CHAVE_DEMONSTRATIVOS = [
    "tipo_documento", "demonstrativo", "cd_cvm", "grupo_dfp",
    "dt_refer", "ordem_exerc", "cd_conta",
]

CHAVE_DMPL = [
    "tipo_documento", "cd_cvm", "grupo_dfp", "dt_refer",
    "ordem_exerc", "cd_conta", "coluna_df",
]


def deduplicar(df, chave, coluna_versao="versao"):
    """
    Remove linhas com a mesma chave de conflito, mantendo a de maior versao.
    Empate de versao: mantém a última ocorrência no arquivo.
    """
    return (
        df.sort_values(coluna_versao, ascending=False)
          .drop_duplicates(subset=chave, keep="first")
          .reset_index(drop=True)
    )

def normalizar_dataframe(
    df: pd.DataFrame,
    tipo_documento: str,
    demonstrativo: str,
    ano: int,
    nome_arquivo: str,
) -> pd.DataFrame:
    """
    Renomeia as colunas do padrão CVM (maiúsculas) para o padrão do
    banco (minúsculas), garante que todas as colunas esperadas existam,
    converte os tipos (datas, números) e adiciona as colunas de
    rastreabilidade que não vêm no CSV original.
    """
    verificar_colunas_mapeadas(df, demonstrativo, ano)
    df = df.rename(columns=MAPA_COLUNAS)

    # Garante todas as colunas esperadas, preenchendo com None as que
    # a fonte não trouxe (ex: dt_ini_exerc/coluna_df para BPA/BPP).
    for coluna in COLUNAS_ESPERADAS:
        if coluna not in df.columns:
            df[coluna] = None
    df = df[COLUNAS_ESPERADAS]

    # BPA/BPP são "fotografias" de um instante só: quando a fonte não traz
    # dt_fim_exerc, usamos dt_refer como equivalente (dt_ini_exerc continua
    # None de propósito -- não existe "início" para um balanço). Feito
    # ANTES da conversão de data abaixo: se fizermos depois, o pandas
    # promove o valor de date para Timestamp ao preencher uma coluna que
    # ficou inteiramente vazia, e os tipos deixam de bater.
    df["dt_fim_exerc"] = df["dt_fim_exerc"].fillna(df["dt_refer"])

    # --- conversões de tipo ---
    # IMPORTANTE: não usar "pd.to_datetime(...).dt.date" e confiar em
    # pd.notnull()/where() para tratar valores ausentes depois. Quando uma
    # coluna inteira fica vazia (ex: dt_ini_exerc em BPA/BPP), o pandas
    # mantém o dtype como datetime64 com NaT em vez de converter para
    # date/None -- e mesmo um .where(pd.notnull(...), None) NÃO consegue
    # substituir esse NaT por None de verdade (o pandas insiste em manter
    # NaT, que é o "nulo nativo" desse dtype). O psycopg2 então recebe um
    # NaT e manda a string literal 'NaT' pro Postgres, que rejeita com
    # "invalid input syntax for type timestamp: NaT". A conversão
    # elemento a elemento abaixo garante Python None de verdade.
    for col_data in ("dt_refer", "dt_ini_exerc", "dt_fim_exerc"):
        convertida = pd.to_datetime(df[col_data], errors="coerce")
        df[col_data] = [d.date() if pd.notna(d) else None for d in convertida]

    df["cd_cvm"] = pd.to_numeric(df["cd_cvm"], errors="coerce").astype("Int64")
    df["versao"] = pd.to_numeric(df["versao"], errors="coerce").fillna(1).astype(int)
    df["vl_conta"] = _converter_valor_numerico(df["vl_conta"])

    # --- colunas de rastreabilidade / controle ---
    df["tipo_documento"] = tipo_documento
    df["demonstrativo"] = demonstrativo  # só é usada na tabela genérica
    df["ano_documento"] = ano
    df["nome_arquivo_origem"] = nome_arquivo

    # descarta linhas sem os campos mínimos obrigatórios (defesa contra CSV malformado)
    obrigatorias = ["cd_cvm", "dt_refer", "cd_conta", "vl_conta"]
    df = df.dropna(subset=obrigatorias)

    return df


# ============================================================
# PASSO 6 — UPSERT NO BANCO (uma função para cada tabela de destino)
# ============================================================
def upsert_demonstrativos(conn, df: pd.DataFrame) -> int:
    """
    Faz upsert em bronze.demonstrativos_valores.
    A cláusula WHERE do DO UPDATE evita que uma versão antiga
    sobrescreva uma versão mais nova, caso os anos sejam processados
    fora de ordem em alguma execução futura.
    """
    if df.empty:
        return 0

    df = deduplicar(df, CHAVE_DEMONSTRATIVOS)

    colunas = [
        "tipo_documento",
        "demonstrativo",
        "cnpj_cia",
        "denom_cia",
        "cd_cvm",
        "grupo_dfp",
        "moeda",
        "escala_moeda",
        "ordem_exerc",
        "dt_refer",
        "versao",
        "dt_ini_exerc",
        "dt_fim_exerc",
        "cd_conta",
        "ds_conta",
        "vl_conta",
        "st_conta_fixa",
        "ano_documento",
        "nome_arquivo_origem",
    ]
    registros = df[colunas].where(pd.notnull(df[colunas]), None).values.tolist()

    sql = f"""
        INSERT INTO bronze.demonstrativos_valores ({", ".join(colunas)}, data_carga)
        VALUES %s
        ON CONFLICT (tipo_documento, demonstrativo, cd_cvm, grupo_dfp,
                      dt_refer, ordem_exerc, cd_conta)
        DO UPDATE SET
            vl_conta   = EXCLUDED.vl_conta,
            versao     = EXCLUDED.versao,
            data_carga = now()
        WHERE EXCLUDED.versao >= bronze.demonstrativos_valores.versao
    """
    template = "(" + ", ".join(["%s"] * len(colunas)) + ", now())"

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, registros, template=template, page_size=1000)
    return len(registros)


def upsert_dmpl(conn, df: pd.DataFrame) -> int:
    """Mesma lógica do upsert acima, mas para bronze.dmpl_valores (tem coluna_df extra)."""
    if df.empty:
        return 0

    df = deduplicar(df, CHAVE_DMPL)

    colunas = [
        "tipo_documento",
        "cnpj_cia",
        "denom_cia",
        "cd_cvm",
        "grupo_dfp",
        "moeda",
        "escala_moeda",
        "ordem_exerc",
        "dt_refer",
        "versao",
        "dt_ini_exerc",
        "dt_fim_exerc",
        "cd_conta",
        "ds_conta",
        "coluna_df",
        "vl_conta",
        "st_conta_fixa",
        "ano_documento",
        "nome_arquivo_origem",
    ]
    registros = df[colunas].where(pd.notnull(df[colunas]), None).values.tolist()

    sql = f"""
        INSERT INTO bronze.dmpl_valores ({", ".join(colunas)}, data_carga)
        VALUES %s
        ON CONFLICT (tipo_documento, cd_cvm, grupo_dfp, dt_refer,
                      ordem_exerc, cd_conta, coluna_df)
        DO UPDATE SET
            vl_conta   = EXCLUDED.vl_conta,
            versao     = EXCLUDED.versao,
            data_carga = now()
        WHERE EXCLUDED.versao >= bronze.dmpl_valores.versao
    """
    template = "(" + ", ".join(["%s"] * len(colunas)) + ", now())"

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, registros, template=template, page_size=1000)
    return len(registros)


# ============================================================
# PASSO 7 — REGISTRAR O RESULTADO EM controle_ingestao
# ============================================================
def registrar_controle(
    conn,
    tipo_documento,
    ano,
    nome_arquivo,
    url,
    hash_arquivo,
    qtd_linhas,
    status,
    mensagem_erro=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze.controle_ingestao
                (tipo_documento, ano_documento, nome_arquivo, url_origem,
                 hash_arquivo, data_processamento, qtd_linhas_inseridas,
                 status, mensagem_erro)
            VALUES (%s, %s, %s, %s, %s, now(), %s, %s, %s)
            ON CONFLICT (tipo_documento, ano_documento, hash_arquivo) DO NOTHING
            """,
            (
                tipo_documento,
                ano,
                nome_arquivo,
                url,
                hash_arquivo,
                qtd_linhas,
                status,
                mensagem_erro,
            ),
        )


# ============================================================
# PASSO 8 — ORQUESTRAÇÃO: PROCESSAR UM (TIPO, ANO)
# ============================================================
def processar_ano(conn, tipo_documento: str, ano: int):
    url, nome_arquivo = montar_url(tipo_documento, ano)
    print(f"[{tipo_documento} {ano}] baixando {url} ...")

    try:
        conteudo = baixar_zip(url)
    except requests.HTTPError as e:
        # Ano ainda não existe (ex: ano futuro) ou problema de rede -- não é erro grave
        print(f"[{tipo_documento} {ano}] arquivo indisponível: {e}")
        return

    hash_atual = calcular_hash(conteudo)

    if hash_ja_processado(conn, tipo_documento, ano, hash_atual):
        print(f"[{tipo_documento} {ano}] sem mudanças desde a última execução. Pulando.")
        return

    try:
        tabelas = extrair_csvs(conteudo, tipo_documento, ano)
        total_linhas = 0

        for demonstrativo, df_bruto in tabelas.items():
            df = normalizar_dataframe(df_bruto, tipo_documento, demonstrativo, ano, nome_arquivo)


            if demonstrativo == DEMONSTRATIVO_DMPL:
                total_linhas += upsert_dmpl(conn, df)
            else:
                total_linhas += upsert_demonstrativos(conn, df)

        registrar_controle(
            conn,
            tipo_documento,
            ano,
            nome_arquivo,
            url,
            hash_atual,
            total_linhas,
            "SUCESSO",
        )
        conn.commit()
        print(f"[{tipo_documento} {ano}] OK — {total_linhas} linhas processadas.")

    except Exception as e:  # noqa: BLE001
        conn.rollback() 
        registrar_controle(
            conn, tipo_documento, ano, nome_arquivo, url, hash_atual, 0, "ERRO", str(e)
        )
        conn.commit()
        print(f"[{tipo_documento} {ano}] ERRO: {e}")


# ============================================================
# PASSO 9 — PONTO DE ENTRADA DO SCRIPT
# ============================================================
def main():
    try:
        conn = get_connection()
    except (OSError, psycopg2.OperationalError) as e:
        print(f"ERRO: não foi possível conectar ao banco. {e}")
        sys.exit(1)

    try:
        # TESTE: só ITR de um ano
        processar_ano(conn, "ITR", 2023)
    finally:
        conn.close()


if __name__ == "__main__":
    main() 