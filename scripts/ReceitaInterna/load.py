from utils.db import gspread_client, push_dataframe

def enviar_google_sheets(df_final, sheet_name: str, worksheet_name: str):
    client = gspread_client()
    push_dataframe(client, df_final, sheet_name, worksheet_name)
