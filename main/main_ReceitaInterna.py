from scripts.ReceitaInterna import extract, config, transform
from utils.logger import setup_logger
from utils.alert_mail import send_email_alert, send_email_error, send_email_sucess
import gc, sys, os
import locale
from datetime import datetime

# Define para português do Brasil
locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

logger = setup_logger("ReceitaInterna")
log_file_path = f"logs/ReceitaInterna_pipeline_{datetime.now().strftime('%Y%m%d')}.log"


def main():
    logger.info("[Main] - 🚀 Iniciando pipeline de campanhas")
    ano_mes_atual = datetime.now().strftime("%b/%Y")
    mes_atual = datetime.now().strftime("%b")

# /////////////////////////////---------------- Extract ----------------\\\\\\\\\\\\\\\\\\\\\\\\
    try:
        #Dados Carregados
        df_meta, df_google, df_planodemidia, df_avisoObras = extract.coletar_planilhas(config.SOURCES, ano_mes_atual)
        logger.info(f"[Main] - Coletas de dados concluidas")
    except Exception as e:
        logger.exception("❌ Erro no pipeline de campanhas - Importação dos dataframes", str(e))
        
        if os.path.exists(log_file_path):
            send_email_error("❌ Erro no pipeline", "Erro detectado na extração dos dados. Log em anexo.", attachments=log_file_path)
        else:
            send_email_alert("❌ Erro no pipeline", "Erro detectado na extração dos dados, mas o arquivo de log não foi encontrado.")
        sys.exit(1)
    finally:
        gc.collect()

# /////////////////////////////---------------- Transform ----------------\\\\\\\\\\\\\\\\\\\\\\\\

    #Receita Interna
    try:
        df_stg_receita = transform.processar_receita(df_planodemidia, df_avisoObras, mes_atual)
        logger.info(f"[Main] - 📊 Processamento de Receita concluído com {len(df_stg_receita)} registros")
    except Exception as e:
        logger.error(f"[Main] - Erro ao processar receita: {e}")
        if os.path.exists(log_file_path):
            send_email_error("❌ Erro no pipeline", "Erro detectado no tratamento de dados. Log em anexo.", attachments=log_file_path)
        else:
            send_email_alert("❌ Erro no pipeline", "Erro detectado no tratamaneto de dados, mas o arquivo de log não foi encontrado.")
        sys.exit(1)
        raise
    finally:
        gc.collect()

    # Métricas
    try:    
        df_meta, df_google = transform.processar_metricas(df_meta, df_google, ano_mes_atual)
        logger.info(f"[Main] - 📊 Processamento de Métricas concluído com {len(df_meta)} registros do Meta e {len(df_google)} do Google")

    except Exception as e:
        logger.error(f"[Main] - Erro ao processar métricas: {e}")
        if os.path.exists(log_file_path):
            send_email_error("❌ Erro no pipeline", "Erro detectado no tratamento de métricas. Log em anexo.", attachments=log_file_path)
        else:
            send_email_alert("❌ Erro no pipeline", "Erro detectado no tratamaneto de métricas, mas o arquivo de log não foi encontrado.")
        sys.exit(1)
        raise
    finally:
        gc.collect()

        
    #     # df_final = transform.processar_dados(df_meta, df_google, ano_mes_atual)
    #     # load.enviar_google_sheets(df_final, config.SHEET_DESTINO)

    #     logger.info("✅ Pipeline de campanhas finalizado com sucesso")
    #     #send_email_alert("✅ Pipeline executado", "Finalizado com sucesso")
    # except Exception as e:
    #     logger.exception("❌ Erro no pipeline de campanhas")
    #     #send_email_alert("❌ Erro no pipeline", str(e))
    #     sys.exit(1)
    # finally:
    #     gc.collect()





    # send_email_sucess(
    # subject="📊 Resumo Diário - Indicadores",
    # variaveis='',
    # attachments=[log_file_path] if os.path.exists(log_file_path) else None
    # )

if __name__ == "__main__":
    main()
