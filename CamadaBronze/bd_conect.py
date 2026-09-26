"""
Módulo  de conexão com o PostgreSQL deste projeto.

Arquitetura medallion (schemas: bronze, silver, gold).
Configuração via .env: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.

"""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

REQUIRED_VARS = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
SCHEMAS = ("bronze", "silver", "gold")


# ============================================================
# Leitura centralizada do .env
# ============================================================
def _read_env() -> dict:
    missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
    if missing:
        raise OSError(f"Variáveis de ambiente faltando: {', '.join(missing)}. ")
    return {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT"),
        "database": os.getenv("PGDATABASE"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
    }


def _ensure_schemas(conn):
    """Cria os schemas se não existirem."""
    with conn.cursor() as cur:
        for schema in SCHEMAS:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.commit()


# ============================================================
# função de conexão do projeto
# ============================================================
def get_connection():
    """
    Abre uma conexão psycopg2 com o Postgres, lendo as credenciais do .env,
    e garante que os schemas bronze/silver/gold existam.

    Devolve uma conexão psycopg2
    avisos:
      - EnvironmentError  se alguma variável do .env estiver faltando
      - psycopg2.OperationalError  se não conseguir conectar ao banco
    """
    cfg = _read_env()
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        connect_timeout=10,
    )
    _ensure_schemas(conn)
    return conn
