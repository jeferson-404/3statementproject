"""
Testes das funções  do CamadaBronze/load_bronze.py.

Só testa funções que nao usam rede nem banco de dados.
Funções que fazem I/O ficam de fora.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Mesmo ajuste de sys.path do load_bronze.py, para o import funcionar
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from CamadaBronze.load_bronze import (
    calcular_hash,
    montar_url,
    normalizar_dataframe,
    verificar_colunas_mapeadas,
)


# ============================================================
# montar_url
# ============================================================
class TestMontarUrl:
    def test_dfp_2023(self):
        url, nome = montar_url("DFP", 2023)
        assert url == (
            "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
            "dfp_cia_aberta_2023.zip"
        )
        assert nome == "dfp_cia_aberta_2023.zip"

    def test_itr_2010(self):
        url, nome = montar_url("ITR", 2010)
        assert url == (
            "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"
            "itr_cia_aberta_2010.zip"
        )
        assert nome == "itr_cia_aberta_2010.zip"

    def test_tipo_vira_lowercase_no_nome(self):
        _, nome = montar_url("DFP", 2024)
        assert nome.startswith("dfp_")


# ============================================================
# calcular_hash
# ============================================================
class TestCalcularHash:
    def test_deterministico(self):
        assert calcular_hash(b"conteudo") == calcular_hash(b"conteudo")

    def test_diferente_para_conteudos_diferentes(self):
        assert calcular_hash(b"a") != calcular_hash(b"b")

    def test_sha256_tamanho(self):
        h = calcular_hash(b"x")
        assert len(h) == 64

    def test_hash_conhecido(self):
        # sha256 de bytes vazios (valor conhecido)
        assert calcular_hash(b"") == (
            "e3b0c44298fc1c149afbf4c8996fb924"
            "27ae41e4649b934ca495991b7852b855"
        )


# ============================================================
# normalizar_dataframe
# ============================================================
class TestNormalizarDataframe:
    @pytest.fixture
    def df_bpa(self):
        """DataFrame mínimo de BPA (sem dt_ini_exerc nem dt_fim_exerc)."""
        return pd.DataFrame({
            "CNPJ_CIA": ["00.000.000/0001-00"],
            "DENOM_CIA": ["EMPRESA TESTE S.A."],
            "CD_CVM": ["12345"],
            "GRUPO_DFP": ["DF Consolidado - Balanco Patrimonial Ativo"],
            "MOEDA": ["REAL"],
            "ESCALA_MOEDA": ["MIL"],
            "ORDEM_EXERC": ["ULTIMO"],
            "DT_REFER": ["2023-12-31"],
            "VERSAO": ["1"],
            "CD_CONTA": ["1"],
            "DS_CONTA": ["Ativo Total"],
            "VL_CONTA": ["1234.56"],
            "ST_CONTA_FIXA": ["S"],
        })

    @pytest.fixture
    def df_dre(self):
        """DataFrame mínimo de DRE (com dt_ini_exerc e dt_fim_exerc)."""
        return pd.DataFrame({
            "CNPJ_CIA": ["00.000.000/0001-00"],
            "DENOM_CIA": ["EMPRESA TESTE S.A."],
            "CD_CVM": ["12345"],
            "GRUPO_DFP": ["DF Consolidado - Demonstracao do Resultado"],
            "MOEDA": ["REAL"],
            "ESCALA_MOEDA": ["MIL"],
            "ORDEM_EXERC": ["ULTIMO"],
            "DT_REFER": ["2023-12-31"],
            "VERSAO": ["1"],
            "DT_INI_EXERC": ["2023-01-01"],
            "DT_FIM_EXERC": ["2023-12-31"],
            "CD_CONTA": ["3.01"],
            "DS_CONTA": ["Receita de Venda"],
            "VL_CONTA": ["5000.00"],
            "ST_CONTA_FIXA": ["S"],
        })

    def test_renomeia_colunas(self, df_bpa):
        df = normalizar_dataframe(df_bpa, "DFP", "BPA", 2023, "arq.zip")
        assert "cd_cvm" in df.columns
        assert "CD_CVM" not in df.columns

    def test_converte_vl_conta(self, df_bpa):
        df = normalizar_dataframe(df_bpa, "DFP", "BPA", 2023, "arq.zip")
        assert df["vl_conta"].iloc[0] == pytest.approx(1234.56)

    def test_cria_colunas_ausentes_como_none(self, df_bpa):
        df = normalizar_dataframe(df_bpa, "DFP", "BPA", 2023, "arq.zip")
        assert "dt_ini_exerc" in df.columns
        assert pd.isna(df["dt_ini_exerc"].iloc[0])

    def test_preenche_dt_fim_exerc_com_dt_refer_para_bpa(self, df_bpa):
        df = normalizar_dataframe(df_bpa, "DFP", "BPA", 2023, "arq.zip")
        assert df["dt_fim_exerc"].iloc[0] == df["dt_refer"].iloc[0]

    def test_nao_altera_dt_fim_exerc_para_dre(self, df_dre):
        df = normalizar_dataframe(df_dre, "DFP", "DRE", 2023, "arq.zip")
        assert df["dt_fim_exerc"].iloc[0] == df["dt_refer"].iloc[0]
        assert df["dt_ini_exerc"].iloc[0].year == 2023

    def test_adiciona_colunas_de_controle(self, df_bpa):
        df = normalizar_dataframe(df_bpa, "DFP", "BPA", 2023, "arq.zip")
        assert df["tipo_documento"].iloc[0] == "DFP"
        assert df["demonstrativo"].iloc[0] == "BPA"
        assert df["ano_documento"].iloc[0] == 2023
        assert df["nome_arquivo_origem"].iloc[0] == "arq.zip"


# ============================================================
# verificar_colunas_mapeadas
# ============================================================
class TestVerificarColunas:
    def test_sem_aviso_quando_tudo_mapeado(self, capsys):
        df = pd.DataFrame({"CNPJ_CIA": ["x"], "CD_CVM": ["1"]})
        verificar_colunas_mapeadas(df, "BPA", 2023)
        assert "AVISO" not in capsys.readouterr().out

    def test_avisa_coluna_desconhecida(self, capsys):
        df = pd.DataFrame({"CNPJ_CIA": ["x"], "COLUNA_NOVA": ["y"]})
        verificar_colunas_mapeadas(df, "BPA", 2023)
        out = capsys.readouterr().out
        assert "AVISO" in out
        assert "COLUNA_NOVA" in out