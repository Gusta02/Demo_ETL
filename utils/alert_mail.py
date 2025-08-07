import yagmail
from jinja2 import Template
from typing import List, Union
from datetime import datetime
import pandas as pd
import os

user = os.getenv("EMAIL_USER")
password = os.getenv("EMAIL_PASSWORD")
recipient = os.getenv("EMAIL_RECIPIENT")

# Alerta de Email
def send_email_alert(subject: str, body: str):

    if not all([user, password, recipient]):
        return

    yag = yagmail.SMTP(user, password)
    yag.send(to=recipient, subject=subject, contents=body)


# Email de Error
def send_email_error(subject: str, body: str, attachments=None):

    if not all([user, password, recipient]):
        return
    
    yag = yagmail.SMTP(user, password)
    yag.send(to=recipient, subject=subject, contents=body, attachments=attachments)


# Mensagem de Conclusão por Email
# V2 //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
def send_email_sucess(
    subject: str,
    dataframes: dict = None,
    variaveis: dict = None,
    status: str = "✅ Sucesso",
    erro: str = None,
    template_path: str = None,
    destinatarios: list = None,
    attachments=None
):
    import os

    # Caminho seguro para o template
    if template_path is None:
        template_path = os.path.join(os.path.dirname(__file__), "template", "mail_report.html")

    with open(template_path, "r", encoding="utf-8") as f:
        template = Template(f.read())

    # Tabelas em HTML
    tabelas_html = ""
    if dataframes:
        for nome, df in dataframes.items():
            tabelas_html += f"<h3>{nome}</h3>" + df.to_html(index=False, border=0)

    # Variáveis em HTML
    variaveis_html = ""
    if variaveis:
        variaveis_html += "<ul>"
        for nome, valor in variaveis.items():
            variaveis_html += f"<li><b>{nome}</b>: {valor}</li>"
        variaveis_html += "</ul>"

    # Renderiza o HTML
    html_renderizado = template.render(
        data=datetime.now().strftime("%d/%m/%Y %H:%M"),
        tabelas_html=tabelas_html,
        variaveis_html=variaveis_html,
        status=status,
        erro=erro
    )

    yag = yagmail.SMTP(user, password)
    yag.send(
        to=destinatarios or [recipient],
        subject=subject,
        contents=html_renderizado,
        attachments=attachments
    )
# V1//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# def send_email_sucess( subject: str, mensagem: str, dataframes: List[Union[pd.DataFrame, tuple]] = [], attachments=None):
#     """
#     Envia e-mail com resumo de dados e/ou anexos.
    
#     - dataframes: Lista de DataFrames ou tuplas (nome_tabela, DataFrame)
#     - anexos: Caminhos de arquivos para anexar (opcional)
#     - para: E-mail do destinatário (padrão: destinatario@email.com)
#     """
#     yag = yagmail.SMTP(user, password)

#     conteudo = [mensagem, "<br><br>"]

#     for i, df in enumerate(dataframes):
#         if isinstance(df, tuple):
#             titulo, tabela = df
#         else:
#             titulo = f"Tabela {i+1}"
#             tabela = df

#         conteudo.append(f"<b>{titulo}</b><br>")
#         conteudo.append(tabela.to_html(index=False, border=1))
#         conteudo.append("<br>")

#     # Anexos
#     yag.send(
#         to=recipient,
#         subject=subject,
#         contents=conteudo,
#         attachments=attachments
#     )


