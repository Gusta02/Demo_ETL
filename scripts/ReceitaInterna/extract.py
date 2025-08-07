import pandas as pd
from utils.db import fetch_sheet_csv
from utils.transform_helpers import cleanup_currency
from datetime import datetime

def coletar_planilhas(sources: dict, mes_atual: str):


    # Dados metricas
    df_meta = fetch_sheet_csv(*sources["meta"])
    df_google = fetch_sheet_csv(*sources["google"])

    # Dados Orçamento
    df_Orcamento = fetch_sheet_csv(*sources["orcamento"])

    # Dados Receita
    df_planoMidia = fetch_sheet_csv(*sources["PlanoDeMidia"])
    df_avisoobras = fetch_sheet_csv(*sources["AvisoObras"])


    return df_meta, df_google, df_Orcamento, df_planoMidia, df_avisoobras
