import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import os

def fetch_sheet_csv(sheet_id: str, sheet_name: str, **read_csv_kwargs):
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    return pd.read_csv(url, **read_csv_kwargs)

def gspread_client():
    creds_path = os.getenv("GSHEET_CREDS", "config/credenciais.json")
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scopes)
    return gspread.authorize(creds)

def push_dataframe(client, df, sheet_name, worksheet_name):
    ws = client.open(sheet_name).worksheet(worksheet_name)
    ws.clear()
    set_with_dataframe(ws, df)
