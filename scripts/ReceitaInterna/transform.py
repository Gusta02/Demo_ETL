import pandas as pd
import numpy as np
from utils.transform_helpers import cleanup_currency
from datetime import datetime

def processar_metricas(df_meta, df_google, mes_atual):
    #Pré limpeza
    df_meta["Amount Spent"] = cleanup_currency(df_meta["Amount Spent"])
    df_google["Cost (Spend)"] = cleanup_currency(df_google["Cost (Spend)"])

    df_meta["Day"] = pd.to_datetime(df_meta["Day"], errors="coerce")
    df_google["Day"] = pd.to_datetime(df_google["Day"], errors="coerce")

    df_meta["Mes"] = df_meta["Day"].dt.strftime("%b/%Y")
    df_meta = df_meta[df_meta["Mes"] == mes_atual]

    df_google["Mes"] = df_google["Day"].dt.strftime("%b/%Y")
    df_google = df_google[df_google["Mes"] == mes_atual]

    return df_meta, df_google

def processar_receita(df_planoMidia, df_avisoobras):

    df_avisoobras = df_avisoobras[["dt_Mes", "vl_Investido"]]

    df_avisoobras.rename(columns={
    "vl_Investido": "vl_InvestimentoClienteTotal"
    }, inplace=True)

    df_avisoobras['nm_Cliente'] = 'Sanasa'
    df_avisoobras['nm_PlanodeMidia'] = ''
    df_avisoobras['vl_InvestimentoMeta']	= 0
    df_avisoobras['vl_InvestimentoFacebook']	= 0
    df_avisoobras['vl_InvestimentoInstagram']	= 0
    df_avisoobras['vl_InvestimentoGoogle']	= 0
    df_avisoobras['vl_InvestimentoYoutube']	= 0
    df_avisoobras['vl_InvestimentoTaboola']	= 0
    df_avisoobras['vl_InvestimentoSpotify'] = 0

    df_planoMidia = df_planoMidia[[
        'dt_Mes', 'nm_Cliente',	'nm_PlanodeMidia',	
        'vl_InvestimentoClienteTotal',	'vl_InvestimentoMeta',	
        'vl_InvestimentoFacebook',	'vl_InvestimentoInstagram',	
        'vl_InvestimentoGoogle',	'vl_InvestimentoYoutube',	
        'vl_InvestimentoTaboola',	'vl_InvestimentoSpotify'
    ]]

    # Concat final
    df_stg_receita = pd.concat([df_planoMidia, df_avisoobras], ignore_index=True)
    return df_stg_receita

def processar_orcamento(df_orcamento):
    pass

def processar_dados(df_meta, df_google, mes_atual):
    df_meta_grouped = df_meta.groupby(["Day", "Account Name", "Campaign Name"]).agg({
        "Amount Spent": "sum", "Link Clicks": "sum", "Impressions": "sum"
    }).reset_index()

    df_google_grouped = df_google.groupby(["Day", "Account Name", "Campaign Name"]).agg({
        "Cost (Spend)": "sum", "Clicks": "sum", "Impressions": "sum"
    }).reset_index()

    df_google_grouped.rename(columns={"Cost (Spend)": "Amount Spent", "Clicks": "Link Clicks"}, inplace=True)

    df_final = pd.concat([df_meta_grouped, df_google_grouped], ignore_index=True)
    return df_final
