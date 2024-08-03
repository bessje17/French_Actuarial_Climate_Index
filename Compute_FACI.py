# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 15:04:21 2024

@author: Jeanne
"""
import plotly.graph_objs as go
import plotly.io as pio
import pandas as pd
import os
os.chdir('C:/Users/Jeanne/Documents/MS/Mémoire/Data MeteoFrance/FACI')
# import requests
from Functions import calculate_percentile, compute_percentile
# from fetch_daily_data_meteo_gouv import fetch_meteo_data, fetch_meteo_data_latest
# # URL du fichier Python brut
# url = "https://github.com/bessje17/French_Actuarial_Climate_Index/raw/main/fetch_daily_data_meteo_gouv.py"

# # Télécharger le contenu du fichier
# response = requests.get(url)
# code = response.text

# # Créer un dictionnaire pour l'espace de noms
# namespace = {}

# # Exécuter le code dans cet espace de noms
# exec(code, namespace)

# # Importer la fonction souhaitée
# fetch_meteo_data = namespace['fetch_meteo_data']
# fetch_meteo_data_latest = namespace['fetch_meteo_data_latest']


s_main = "s_main"
if s_main == "s_main":

    # 1. Get climate daily data
    url = "https://github.com/bessje17/French_Actuarial_Climate_Index/raw/main/Data_RR_TX_TN_1961-2024.xlsx"
    df_meteo = pd.read_excel(url, index_col=0)

    # 2. Reference period
    df_reference = df_meteo[
        (df_meteo['DATE'].dt.year >= 1961) &
        (df_meteo['DATE'].dt.year <= 1990)]

    # 3. Compute daily percentile based on the reference period

    # !!! 13055029 - Marseille pas présent à l'epoque, apparait en 2014
    df_TX90 = df_reference.groupby('NUM_POSTE').apply(calculate_percentile,
                                                      s_col='TX',
                                                      f_quantile=0.9)
    df_TX10 = df_reference.groupby('NUM_POSTE').apply(calculate_percentile,
                                                      s_col='TX',
                                                      f_quantile=0.1)

    df_TN90 = df_reference.groupby('NUM_POSTE').apply(calculate_percentile,
                                                      s_col='TN',
                                                      f_quantile=0.9)
    df_TN10 = df_reference.groupby('NUM_POSTE').apply(calculate_percentile,
                                                      s_col='TN',
                                                      f_quantile=0.1)
    df_TX_TN = df_reference.groupby('NUM_POSTE').apply(compute_percentile,
                                                      l_col=['TX','TN'],
                                                      f_quantile1=0.9,
                                                      f_quantile2=0.1)
    # 4. Compute Hight temp component

    # Extraire le jour de l'année
    df_meteo['JOUR_ANNEE'] = df_meteo['DATE'].dt.dayofyear

    # Transformer df_TX et df_TN en format long avec melt
    df_TX90 = df_TX90.reset_index()
    df_TN90 = df_TN90.reset_index()

    df_TX90 = df_TX90.rename(columns={'index': 'NUM_POSTE'})
    df_TN90 = df_TN90.rename(columns={'index': 'NUM_POSTE'})

    df_TX90 = df_TX90.melt(id_vars='NUM_POSTE', var_name='JOUR_ANNEE',
                           value_name='TX90')
    df_TN90 = df_TN90.melt(id_vars='NUM_POSTE', var_name='JOUR_ANNEE',
                           value_name='TN90')

    # Joindre les dataframes
    df_meteo = df_meteo.merge(df_TX90, on=['NUM_POSTE', 'JOUR_ANNEE'],
                              how='left')
    df_meteo = df_meteo.merge(df_TN90, on=['NUM_POSTE', 'JOUR_ANNEE'],
                              how='left')

    # Supprimer la colonne JOUR_ANNEE
    df_meteo = df_meteo.drop(columns=['JOUR_ANNEE'])

    # Créer une colonne "Mois/Année" à partir de "DATE"
    df_meteo['MOIS_ANNEE'] = df_meteo['DATE'].dt.to_period('M')

    # Comparer les valeurs "TX" et "TX90" et créer une colonne booléenne
    df_meteo['TX_SUP_TX90'] = df_meteo['TX'] > df_meteo['TX90']
    df_meteo['TN_SUP_TN90'] = df_meteo['TN'] > df_meteo['TN90']

    # Grouper par "MOIS_ANNEE" et "NUM_POSTE" &
    # compter les fois où "TX_SUP_TX90" sont True
    df_monthly_TX90 = df_meteo.groupby(['MOIS_ANNEE', 'NUM_POSTE'])[
        'TX_SUP_TX90'].sum().unstack(fill_value=0)
    df_monthly_TN90 = df_meteo.groupby(['MOIS_ANNEE', 'NUM_POSTE'])[
        'TN_SUP_TN90'].sum().unstack(fill_value=0)
    # Calcul de T90 = moyenne de TX90 et TN90
    df_monthly_T90 = pd.concat([df_monthly_TX90, df_monthly_TN90],
                               axis=1).groupby(level=0, axis=1).mean()

    # Filtrer les données pour les années 1961 à 1990
    start_period = '1961-01'
    end_period = '1990-01'
    df_monthly_T90_1961_1990 = df_monthly_T90.loc[start_period:end_period]

    # Calculer la moyenne et l'écart-type pour chaque NUM_POSTE sur la
    # période de reference
    mean = df_monthly_T90_1961_1990.mean()
    std_dev = df_monthly_T90_1961_1990.std()

    # Appliquer la standardisation : (valeur - moyenne) / écart-type
    df_monthly_T90_std = (df_monthly_T90 - mean) / std_dev

    # Moyenne des T90 pour toutes les stations
    df_monthly_T90_std_agg = df_monthly_T90_std.mean(axis=1)
    # Moyenne glissante
    df_monthly_T90_std_agg_rolling = df_monthly_T90_std_agg.rolling(
        window=30, min_periods=1).mean()

    # 5. Compute low temp component

    df_meteo['JOUR_ANNEE'] = df_meteo['DATE'].dt.dayofyear
    # Transformer df_TX et df_TN en format long avec melt
    df_TX10 = df_TX10.reset_index()
    df_TN10 = df_TN10.reset_index()
    df_TX10 = df_TX10.rename(columns={'index': 'NUM_POSTE'})
    df_TN10 = df_TN10.rename(columns={'index': 'NUM_POSTE'})

    df_TX10 = df_TX10.melt(id_vars='NUM_POSTE', var_name='JOUR_ANNEE',
                           value_name='TX10')
    df_TN10 = df_TN10.melt(id_vars='NUM_POSTE', var_name='JOUR_ANNEE',
                           value_name='TN10')

    # Joindre les dataframes
    df_meteo = df_meteo.merge(df_TX10, on=['NUM_POSTE', 'JOUR_ANNEE'],
                              how='left')
    df_meteo = df_meteo.merge(df_TN10, on=['NUM_POSTE', 'JOUR_ANNEE'],
                              how='left')

    # Supprimer la colonne JOUR_ANNEE
    df_meteo = df_meteo.drop(columns=['JOUR_ANNEE'])

    # Créer une colonne "Mois/Année" à partir de "DATE"
    df_meteo['MOIS_ANNEE'] = df_meteo['DATE'].dt.to_period('M')

    # Comparer les valeurs "TX" et "TX10" et créer une colonne booléenne
    df_meteo['TX_SUP_TX10'] = df_meteo['TX'] < df_meteo['TX10']
    df_meteo['TN_SUP_TN10'] = df_meteo['TN'] < df_meteo['TN10']

    # Grouper par "MOIS_ANNEE" et "NUM_POSTE"
    # &  compter les valeurs de "TX_SUP_TX10" qui sont True
    df_monthly_TX10 = df_meteo.groupby(['MOIS_ANNEE', 'NUM_POSTE'])[
        'TX_SUP_TX10'].sum().unstack(fill_value=0)
    df_monthly_TN10 = df_meteo.groupby(['MOIS_ANNEE', 'NUM_POSTE'])[
        'TN_SUP_TN10'].sum().unstack(fill_value=0)
    df_monthly_T10 = pd.concat([df_monthly_TX10, df_monthly_TN10],
                               axis=1).groupby(level=0, axis=1).mean()

    # Filtrer les lignes de df_monthly_T10 pour la plage de périodes
    start_period = '1961-01'
    end_period = '1990-01'

    df_monthly_T10_1961_1910 = df_monthly_T10.loc[start_period:end_period]

    # Calculer la moyenne et l'écart-type pour chaque NUM_POSTE sur la
    # période de reference
    mean = df_monthly_T10_1961_1910.mean()
    std_dev = df_monthly_T10_1961_1910.std()

    # Appliquer la standardisation : (valeur - moyenne) / écart-type
    df_monthly_T10_std = (df_monthly_T10 - mean) / std_dev

    # Moyenne des T10 pour toutes les stations
    df_monthly_T10_std_agg = df_monthly_T10_std.mean(axis=1)
    # Moyenne glissante
    df_monthly_T10_std_agg_rolling = df_monthly_T10_std_agg.rolling(
        window=30, min_periods=1).mean()


# Créer une figure
fig = go.Figure()

# Ajouter la trace de la série
fig.add_trace(go.Scatter(
    x=df_monthly_T90_std_agg_rolling.index.to_timestamp(),
    y=df_monthly_T90_std_agg_rolling.values,
    mode='lines',
    name='Composite T90_std',
    line=dict(color='red')
))

fig.add_trace(go.Scatter(
    x=df_monthly_T10_std_agg_rolling.index.to_timestamp(),
    y=df_monthly_T10_std_agg_rolling.values,
    mode='lines',
    name='Composite T10_std',
    line=dict(color='blue')
))

fig.add_trace(go.Scatter(
    x=df_monthly_T10_std_agg_rolling.index.to_timestamp(),
    y=pd.Series(df_monthly_T90_std_agg_rolling -
                df_monthly_T10_std_agg_rolling).values,
    mode='lines',
    name='Difference',
    line=dict(color='black')
))

# Personnaliser la figure
fig.update_layout(
    title="Composantes températures hautes et basses de l'index climatique actuariel",
    xaxis_title='Date',
    yaxis_title='Valeur',
    template='plotly'
)
# Afficher la figure
pio.show(fig)
fig.write_html('T90_10std.html')





    




