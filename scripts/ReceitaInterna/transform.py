import pandas as pd
import numpy as np
import gc, os, sys
from utils.transform_helpers import cleanup_currency, criar_tipo_campanha, padronizar_platform, traduzir_status, corrigir_plataforma
from utils.logger import setup_logger
from datetime import datetime

logger = setup_logger("ReceitaInterna")

def processar_receita(df_planoMidia, df_avisoobras, mes_atual):

    try:
        df_avisoobras = df_avisoobras[["dt_Mes", "vl_Investido"]]

        df_avisoobras.rename(columns={
        "vl_Investido": "vl_InvestimentoClienteTotal"
        }, inplace=True)

        df_avisoobras['nm_Cliente'] = 'Sanasa'
        df_avisoobras['nm_PlanodeMidia'] = ''
        df_avisoobras['vl_InvestimentoMeta']	= 0
        df_avisoobras['vl_InvestimentoFacebook'] = 0
        df_avisoobras['vl_InvestimentoInstagram'] = 0
        df_avisoobras['vl_InvestimentoGoogle']	= 0
        df_avisoobras['vl_InvestimentoYoutube']	= 0
        df_avisoobras['vl_InvestimentoTaboola']	= 0
        df_avisoobras['vl_InvestimentoSpotify'] = 0

        df_avisoobras = df_avisoobras[df_avisoobras["dt_Mes"].str.lower() == mes_atual.lower()]

        logger.info(f"[Transform] - 📊 Padronização de {len(df_avisoobras)} registros do Aviso de Obras concluída")

        df_planoMidia = df_planoMidia[[
            'dt_Mes', 'nm_Cliente',	'nm_PlanodeMidia',	
            'vl_InvestimentoClienteTotal',	'vl_InvestimentoMeta',	
            'vl_InvestimentoFacebook',	'vl_InvestimentoInstagram',	
            'vl_InvestimentoGoogle',	'vl_InvestimentoYoutube',	
            'vl_InvestimentoTaboola',	'vl_InvestimentoSpotify'
        ]]

        df_planoMidia = df_planoMidia[df_planoMidia["dt_Mes"].str.lower() == mes_atual.lower()]
        logger.info(f"[Transform] - 📊 Padronização de {len(df_planoMidia)} registros do Plano de Mídia concluída")
    except Exception as e:
        logger.error(f"[Transform] - Erro ao padronizar bases: {e}")
        raise
    finally:
        gc.collect()

    try:
        # Concat final
        df_stg_receita = pd.concat([df_planoMidia, df_avisoobras], ignore_index=True)
        logger.info(f"[Transform] - 📊 Concatenação de {len(df_stg_receita)} registros de receita concluída")
    except Exception as e:
        logger.error(f"[Transform] - Erro ao concatenar bases de receita: {e}")
        raise
    finally:
        gc.collect()

    try:
        df_stg_receita.rename(columns={"dt_Mes": "Mes", "nm_Cliente": "Cliente"}, inplace=True)
        logger.info(f"[Transform] - 📊 Renomeação de colunas concluída")

        colunas_para_converter = [
            'vl_InvestimentoClienteTotal', 'vl_InvestimentoMeta', 'vl_InvestimentoFacebook',
            'vl_InvestimentoInstagram', 'vl_InvestimentoGoogle', 'vl_InvestimentoYoutube',
            'vl_InvestimentoTaboola', 'vl_InvestimentoSpotify'
        ]

        for col in colunas_para_converter:
            if col in df_stg_receita.columns:
                df_stg_receita[col] = cleanup_currency(df_stg_receita[col])
        logger.info(f"[Transform] - 📊 Conversão de colunas monetárias concluída")
    except Exception as e:
        logger.error(f"[Transform] - Erro ao renomear ou converter colunas: {e}")
        raise
    finally:
        gc.collect()

    return df_stg_receita

def processar_metricas_Meta(df_meta, ano_mes_atual):

    # ////////////////////-----------------------------  Tratamento META -----------------------------\\\\\\\\\\\\\\\\\\\\\\\\\\\
    try:
        df_Meta = df_meta[[
            "Day", "Account Name", "Campaign Name", "Campaign Status", "Platform", "Ad Name", "Ad Set Name", "Amount Spent", 
            "Impressions", "Link Clicks", "Reach", "On-Facebook Leads", "Leads"
        ]]

        df_Meta["Day"] = pd.to_datetime(df_Meta["Day"])

        # Criar a coluna "Mes" no formato "Mai/2025"
        df_Meta["Mes_ano"] = df_Meta["Day"].dt.strftime("%b/%Y")

        df_Meta["Amount Spent"] = cleanup_currency(df_Meta["Amount Spent"])

        df_Meta = (df_Meta
            .pipe(criar_tipo_campanha)
            .pipe(padronizar_platform)
            .pipe(traduzir_status)
            .pipe(corrigir_plataforma)
        )
        logger.info(f"[Transform] - 📊 Filtragem e Tratamento de {len(df_Meta)} registros do Meta para o mês {ano_mes_atual} concluída")
    except Exception as e:
        logger.error(f"[Transform] - Erro ao filtrar e tratar dados do Meta: {e}")
        raise
    finally:
        gc.collect()

    try:
        # Faz o agrupamento com agregações mistas: sum e nunique
        df_Meta_agroup = df_Meta.groupby(["Day", "Mes_ano", "Account Name", "Campaign Name", "Plataforma Correta", "Campaign Status"], as_index=False).agg({
            "Amount Spent": "sum",
            "Impressions": "sum",
            "Link Clicks": "sum",
            "Reach": "sum", 
            "On-Facebook Leads": "sum",
            "Leads": "sum",
            "Ad Name": pd.Series.nunique,
            "Ad Set Name": pd.Series.nunique
        })

        # (Opcional) Renomeia as colunas de contagem para deixar mais claro
        df_Meta_agroup.rename(columns={
            "Ad Name": "Unique Ad Names",
            "Ad Set Name": "Unique Ad Set Names"
        }, inplace=True)

        logger.info(f"[Transform] - 📊 Agrupamento de {len(df_Meta_agroup)} registros do Meta concluído")
    except Exception as e:
        logger.error(f"[Transform] - Erro ao agrupar dados do Meta: {e}")
        raise
    finally:
        gc.collect()

    # ////////////////////-----------------------------  Tratamento Meta Visão Mes -----------------------------\\\\\\\\\\\\\\\\\\\\\\\\\\\
    
    try:
        # Faz o agrupamento com agregações mistas: sum e nunique
        df_Meta_Mes_agroup = df_Meta.groupby(["Mes", "Account Name", "Campaign Name", "Plataforma Correta", "Campaign Status"], as_index=False).agg({
            "Amount Spent": "sum",
            "Impressions": "sum",
            "Link Clicks": "sum",
            "Reach": "sum",
            "On-Facebook Leads": "sum",
            "Leads": "sum",
            "Ad Name": pd.Series.nunique,
            "Ad Set Name": pd.Series.nunique
        })

        # (Opcional) Renomeia as colunas de contagem para deixar mais claro
        df_Meta_Mes_agroup.rename(columns={
            "Ad Name": "Unique Ad Names",
            "Ad Set Name": "Unique Ad Set Names"
        }, inplace=True)
        logger.info(f"[Transform] - 📊 Agrupamento de {len(df_Meta_Mes_agroup)} registros do Meta visão mês concluído")

    except Exception as e:
        logger.error(f"[Transform] - Erro ao agrupar dados do Meta visão mês: {e}")
        raise

    finally:
        gc.collect()

    return df_Meta_agroup, df_Meta_Mes_agroup

    # ////////////////////-----------------------------  Tratamento Google -----------------------------\\\\\\\\\\\\\\\\\\\\\\\\\\\
def processar_metricas_Google(df_google, ano_mes_atual):
    try:
        df_Google = df_google[[
            "Day","Account Name","Campaign Name","Campaign state","Advertising Channel","Video Views",
            "Clicks","Impressions","CTR","Avg. CPC","Cost (Spend)"
        ]]
 
        logger.info(f"[Transform] - 📊 Filtragem de {len(df_Google)} registros do Meta para o mês {ano_mes_atual} concluída")
    except Exception as e:
        logger.error(f"[Transform] - Erro ao filtrar dados do Google: {e}")
        raise
    return df_Google



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
