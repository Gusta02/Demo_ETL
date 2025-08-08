import pandas as pd
from utils.db import fetch_sheet_csv
from utils.transform_helpers import cleanup_currency
from utils.logger import setup_logger
from datetime import datetime
import gc, sys, os

logger = setup_logger("ReceitaInterna")

def coletar_planilhas(sources: dict, mes_atual: str):

    try:
        # Dados metricas
        df_meta = fetch_sheet_csv(*sources["meta"])
        logger.info(f"[Extract] - 📊 Coletados {len(df_meta)} registros do Meta")
        df_google = fetch_sheet_csv(*sources["google"])
        logger.info(f"[Extract] - 📊 Coletados {len(df_google)} registros do Google")
    except Exception as e:
        logger.error(f"[Extract] - Erro ao coletar dados de métricas: {e}")
        raise
    finally:
        gc.collect()

    # try:
    #     # Dados Orçamento
    #     df_Orcamento = fetch_sheet_csv(*sources["orcamento"])
    #     logger.info(f"[Extract] - 📊 Coletados {len(df_Orcamento)} registros do Orçamento")

    # except Exception as e:
    #     logger.error(f"[Extract] - Erro ao coletar dados de orçamento: {e}")
    #     raise
    # finally:
    #     gc.collect()

    try:
        # Dados Receita
        df_avisoObras = fetch_sheet_csv(*sources["AvisoObras"])
        logger.info(f"📊 Coletados {len(df_avisoObras)} registros do Aviso de Obras")

        df_planodemidia = fetch_sheet_csv(*sources["PlanoDeMidia"])
        logger.info(f"📊 Coletados {len(df_planodemidia)} registros do Plano de Midia")

    except Exception as e:
        logger.error(f"Erro ao coletar dados de receita: {e}")
        raise
    finally:
        gc.collect()


    return df_meta, df_google, df_planodemidia, df_avisoObras
