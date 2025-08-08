import pandas as pd
import numpy as np

def cleanup_currency(coluna):
    """
    Converte strings monetárias como '1.234,56' para float32: 1234.56.
    Remove espaços, símbolos e adapta vírgula para ponto.
    """
    return (
        coluna.astype(str)
              .str.replace("\xa0", "", regex=False)
              .str.replace(" ", "", regex=False)
              .str.replace(",", ".", regex=False)
              .apply(lambda x: float(x) if x.replace('.', '', 1).isdigit() else 0.0)
    )


def zero_fill(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Preenche colunas ausentes em um DataFrame com zeros (float).
    Útil para garantir estrutura antes de concatenação.
    """
    for col in cols:
        if col not in df.columns:
            df[col] = 0.0
    return df


def normalize_text(series: pd.Series) -> pd.Series:
    """
    Remove espaços extras e padroniza texto para letras minúsculas.
    """
    return series.astype(str).str.strip().str.lower()


def format_date(series: pd.Series, format_out: str = "%Y-%m-%d") -> pd.Series:
    """
    Converte colunas de data para o formato desejado (padrão: 'YYYY-MM-DD').
    Datas inválidas serão convertidas em NaT.
    """
    return pd.to_datetime(series, errors="coerce").dt.strftime(format_out)


def remove_duplicates(df: pd.DataFrame, subset: list[str] = None) -> pd.DataFrame:
    """
    Remove linhas duplicadas de um DataFrame com base nas colunas informadas.
    Se subset for None, remove duplicatas completas.
    """
    return df.drop_duplicates(subset=subset)


def replace_empty_with_nan(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Substitui valores vazios ou strings vazias por np.nan nas colunas especificadas.
    """
    for col in cols:
        df[col] = df[col].replace("", np.nan)
    return df

def criar_tipo_campanha(df: pd.DataFrame) -> pd.DataFrame:
    """Cria a coluna 'tipo campanha' com base no nome da campanha."""
    df["tipo campanha"] = np.where(
        df["Campaign Name"].str.contains("Facebook", case=False, na=False),
        "Facebook",
        np.where(
            df["Campaign Name"].str.contains("Instagram", case=False, na=False),
            "Instagram",
            "Meta"
        )
    )
    return df

def padronizar_platform(df: pd.DataFrame) -> pd.DataFrame:
    """Substitui valores de Platform para nomes padronizados."""
    df["Platform"] = df["Platform"].replace({
        "Audience Network": "Facebook",
        "Messenger": "Facebook"
    })
    return df

def traduzir_status(df: pd.DataFrame) -> pd.DataFrame:
    """Traduz status da campanha para português."""
    df["Campaign Status"] = df["Campaign Status"].replace({
        "PAUSED": "Desativado",
        "ACTIVE": "Ativo"
    })
    return df

def corrigir_plataforma(df: pd.DataFrame) -> pd.DataFrame:
    """Cria a coluna 'Plataforma Correta' com base no tipo de campanha."""
    df["Plataforma Correta"] = np.where(
        df["tipo campanha"] == "Meta",
        df["Platform"],
        np.where(
            df["tipo campanha"] != df["Platform"],
            df["tipo campanha"],
            df["Platform"]
        )
    )
    return df