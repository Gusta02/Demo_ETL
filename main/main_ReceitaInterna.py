from scripts.ReceitaInterna import extract, config
from utils.logger import setup_logger
from utils.alert_mail import send_email_alert, send_email_error, send_email_sucess
import gc, sys, os
from datetime import datetime

logger = setup_logger("ReceitaInterna")
log_file_path = f"logs/ReceitaInterna_pipeline_{datetime.now().strftime('%Y%m%d')}.log"

def main():
    logger.info("🚀 Iniciando pipeline de campanhas")
    mes_atual = datetime.now().strftime("%b/%Y")

    try:
        #Dados Carregados
        df_meta, df_google, df_orcamento, df_planodemidia, df_avisoObras = extract.coletar_planilhas(config.SOURCES, mes_atual)
        logger.info(f"📊 Receita Coletada {len(df_avisoObras)} registros do Aviso de Obras e {len(df_planodemidia)} do Plano de Midia")
        logger.info(f"📊 Orçamento Coletada {len(df_orcamento)} registros do Controle")
        logger.info(f"📊 Coletados {len(df_meta)} registros do Meta e {len(df_google)} do Google")
        df_meta_teste = df_meta.head()
        df_google_teste = df_google.head()
    
    except Exception as e:
        logger.exception("❌ Erro no pipeline de campanhas - Importação dos dataframes", str(e))
        
        if os.path.exists(log_file_path):
            send_email_error("❌ Erro no pipeline", "Erro detectado. Log em anexo.", attachments=log_file_path)
        else:
            send_email_alert("❌ Erro no pipeline", "Erro detectado, mas o arquivo de log não foi encontrado.")
        sys.exit(1)
    finally:
        gc.collect()

    # try:
    #     # df_final = transform.processar_dados(df_meta, df_google, mes_atual)
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
