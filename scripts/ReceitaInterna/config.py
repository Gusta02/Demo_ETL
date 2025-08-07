import numpy as np
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("EMAIL_USER")
ID_META = os.getenv("ID_META")
ID_GOOGLE = os.getenv("ID_GOOGLE")
ID_ORCAMENTO = os.getenv("ID_ORCAMENTO")
ID_AVISO_OBRAS = os.getenv("ID_AVISO_OBRAS")
ID_PLANO_MIDIA = os.getenv("ID_PLANO_MIDIA")

SOURCES = {
    "meta": (ID_META, "Data_Base_Gu_Meta"),
    "google": (ID_GOOGLE, "Data_Base_Gu_Google"),
    "orcamento": (ID_ORCAMENTO, "Data_Base"),
    "AvisoObras": (ID_AVISO_OBRAS, "AvisoDeObrasSanasa"),
    "PlanoDeMidia": (ID_PLANO_MIDIA, "PlanoDeMidia"),
}

SHEET_DESTINO = "BaseDadosCampanha"

