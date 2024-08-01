# -*- coding: utf-8 -*-
"""
Created on Wed Jun 12 15:17:31 2024

@author: jeanne.bessiere
"""

import duckdb
import pandas as pd

def fetch_meteo_data(s_select_fields: str = "*", t_date_range: tuple = None,
                     l_departments: list = None) -> pd.DataFrame:
    """
    Récupère les données météorologiques de base quotidienne sur_
    https://meteo.data.gouv.fr/datasets/6569b51ae64326786e4e8e1a
    en fonction des paramètres spécifiés.

    Parameters :
        select_fields (str): Champs à sélectionner dans la requête SQL.
                            Par défaut, "*" pour sélectionner tous les champs.
        date_range (tuple): Plage de dates de la colonne AAAAMM.
                            Par défaut, None pour inclure toutes les dates.
                            Le tuple doit contenir les dates de début et de_
                            fin au format "YYYYMM".
        departments (list): Liste de codes de départements français.
                            Par défaut, None pour inclure tous les_
                            départements. Chaque code de département doit être_
                            une chaîne de caractères formatée avec deux_
                            chiffres.

    Returns:
        pd.DataFrame: DataFrame contenant les données météorologiques
    """
    # Préparation de la condition de date
    date_condition = ""
    if t_date_range:
        start_date, end_date = t_date_range
        date_condition = f"AND AAAAMMJJ BETWEEN '{start_date}' AND '{end_date}'"

    # Préparation de la requête
    query_template = f"""
    PREPARE EVOL_STATION AS                   
        SELECT CAST(NUM_POSTE AS integer) AS NUM_POSTE,
        {s_select_fields}, strptime(AAAAMMJJ::varchar,'%Y%m%d') AS DATE
        FROM read_csv_auto('https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_'
                            || ? || '_previous-1950-2022_RR-T-Vent.csv.gz')

        WHERE TM IS NOT NULL
        {date_condition}
        ORDER BY DATE DESC

    """

    duckdb.execute(query_template)

    # Liste des départements à traiter, format:chaîne de caractères 2 chiffres
    if l_departments is None:
        l_departments = ['{:02}'.format(i) for i in range(1, 96)]

    # Initialisation du DataFrame
    df_ = pd.DataFrame()

    # Boucle sur chaque département
    for dept in l_departments:
        # Exécution de la requête préparée avec le code du département actuel
        query = f"EXECUTE EVOL_STATION('{dept}')"
        df_temp = duckdb.execute(query).df()
        # ajout d'une colonne avec le numéro du département
        df_temp['NUM_DEPT'] = int(dept)

        # Concaténation des résultats avec le DataFrame existant
        df_ = pd.concat([df_, df_temp])

    return df_


def fetch_meteo_data_latest(s_select_fields: str = "*", t_date_range:
                            tuple = None, l_departments: list = None
                            ) -> pd.DataFrame:
    """
    Récupère les données météorologiques de base quotidienne (2023-2024) sur_
    https://meteo.data.gouv.fr/datasets/6569b51ae64326786e4e8e1a
    en fonction des paramètres spécifiés.

    Parameters :
        select_fields (str): Champs à sélectionner dans la requête SQL.
                            Par défaut, "*" pour sélectionner tous les champs.
        date_range (tuple): Plage de dates de la colonne AAAAMM.
                            Par défaut, None pour inclure toutes les dates.
                            Le tuple doit contenir les dates de début et de_
                            fin au format "YYYYMM".
        departments (list): Liste de codes de départements français.
                            Par défaut, None pour inclure tous les_
                            départements. Chaque code de département doit être_
                            une chaîne de caractères formatée avec deux_
                            chiffres.

    Returns:
        pd.DataFrame: DataFrame contenant les données météorologiques
    """
    # Préparation de la condition de date
    date_condition = ""
    if t_date_range:
        start_date, end_date = t_date_range
        date_condition = f"AND AAAAMMJJ BETWEEN '{start_date}' AND '{end_date}'"

    # Préparation de la requête
    query_template = f"""
    PREPARE EVOL_STATION AS                  
        SELECT CAST(NUM_POSTE AS integer) AS NUM_POSTE,
        {s_select_fields}, strptime(AAAAMMJJ::varchar,'%Y%m%d') AS DATE
        FROM read_csv_auto('https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_'
                            || ? || '_latest-2023-2024_RR-T-Vent.csv.gz')

        WHERE TM IS NOT NULL
        {date_condition}
        ORDER BY DATE DESC

    """

    duckdb.execute(query_template)

    # Liste des départements à traiter, format:chaîne de caractères 2 chiffres
    if l_departments is None:
        l_departments = ['{:02}'.format(i) for i in range(1, 96)]

    # Initialisation du DataFrame
    df_ = pd.DataFrame()

    # Boucle sur chaque département
    for dept in l_departments:
        # Exécution de la requête préparée avec le code du département actuel
        query = f"EXECUTE EVOL_STATION('{dept}')"
        df_temp = duckdb.execute(query).df()
        # ajout d'une colonne avec le numéro du département
        df_temp['NUM_DEPT'] = int(dept)

        # Concaténation des résultats avec le DataFrame existant
        df_ = pd.concat([df_, df_temp])

    return df_

