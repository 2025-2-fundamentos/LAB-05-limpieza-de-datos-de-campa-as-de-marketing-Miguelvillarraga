"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import os

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    input_folder = "files/input/"
    output_folder = "files/output/"
    os.makedirs(output_folder, exist_ok=True)

    client_cols = [
        "client_id",
        "age",
        "job",
        "marital",
        "education",
        "credit_default",
        "mortgage",
    ]
    campaign_cols = [
        "client_id",
        "number_contacts",
        "contact_duration",
        "previous_campaign_contacts",
        "previous_outcome",
        "campaign_outcome",
        "last_contact_date",
    ]
    economics_cols = [
        "client_id",
        "cons_price_idx",
        "euribor_three_months",
    ]

    client_csv = []
    campaign_csv = []
    economics_csv = []

    meses = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }

    for i in range(10):
        file_path = os.path.join(input_folder, f"bank-marketing-campaing-{i}.csv.zip")
        df = pd.read_csv(file_path, compression="zip")
        # Clean client data
        df["job"] = df["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
        df["education"] = df["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
        df["credit_default"] = df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
        df["mortgage"] = df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
        df["previous_outcome"] = df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
        df["campaign_outcome"] = df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0 )
        df["month"] = df["month"].str.lower().map(meses)
        df["last_contact_date"] = pd.to_datetime(dict(year=2022, month=df["month"], day=df["day"]))
        df_1 = df[client_cols].copy()
        df_2 = df[campaign_cols].copy()
        df_3 = df[economics_cols].copy()
        client_csv.append(df_1)
        campaign_csv.append(df_2)
        economics_csv.append(df_3)
    
    client_csv = pd.concat(client_csv, ignore_index=True)
    campaign_csv = pd.concat(campaign_csv, ignore_index=True)
    economics_csv = pd.concat(economics_csv, ignore_index=True)
    client_csv.to_csv(os.path.join(output_folder, "client.csv"), index=False)
    campaign_csv.to_csv(os.path.join(output_folder, "campaign.csv"), index=False)
    economics_csv.to_csv(os.path.join(output_folder, "economics.csv"), index=False)

    return client_csv, campaign_csv, economics_csv


if __name__ == "__main__":
    client_csv, campaign_csv, economics_csv = clean_campaign_data()
    print("Client Data:")
    print(client_csv.head())
    print("\nCampaign Data:")
    print(campaign_csv.head())
    print("\nEconomics Data:")
    print(economics_csv.head())