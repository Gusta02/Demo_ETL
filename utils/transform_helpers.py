import pandas as pd
import numpy as np

def cleanup_currency(series: pd.Series) -> pd.Series:
    """
    Converte strings monetárias como '1.234,56' para float32: 1234.56.
    Remove espaços, símbolos e adapta vírgula para ponto.
    """
    return (
        series.astype(str)
              .str.replace(r"[\s\xa0]", "", regex=True)
              .str.replace(",", ".", regex=False)
              .str.replace(r"[^\d.]", "", regex=True)
              .astype(np.float32)
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
