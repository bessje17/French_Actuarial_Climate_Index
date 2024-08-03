import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# url = "https://github.com/bessje17/French_Actuarial_Climate_Index/raw/main/Data_RR_TX_TN_1961-2024.xlsx"
# df_meteo_full_period = pd.read_excel(url, index_col=0)
   

# Définition des paramètres
start_date = datetime(1961, 1, 1)
end_date = datetime(1992, 12, 31)
num_stations = 5

# Génération de toutes les dates quotidiennes
date_range = pd.date_range(start=start_date, end=end_date, freq='D')

# Génération des identifiants de stations météo
station_ids = np.arange(1, num_stations + 1)

# Fonction pour générer des températures aléatoires
def generate_tx(size):
    return np.random.uniform(5, 40, size)

def generate_tn(size):
    return np.random.uniform(-10, 15, size)

# Fonction pour générer des précipitations aléatoires
def generate_rr(size):
    return np.random.uniform(0, 50, size)

# Calcul du nombre total d'enregistrements
num_records = len(date_range) * num_stations

# Génération des données
tx_values = generate_tx(num_records)
rr_values = generate_rr(num_records)
tn_values = generate_tn(num_records)

# Création du DataFrame
data = {
    "NUM_POSTE": np.tile(station_ids, len(date_range)),
    "DATE": np.repeat(date_range, num_stations),
    "TX": tx_values,
    "RR": rr_values,
    "TN" : tn_values
}

df_meteo_full_period = pd.DataFrame(data)


# Filtrer les données entre 1961 et 1990
df_historique = df_meteo_full_period[
    (df_meteo_full_period['DATE'].dt.year >= 1961) &
    (df_meteo_full_period['DATE'].dt.year <= 1990)
]

# Filtrer les données à partir de 1990
df_1990_onwards = df_meteo_full_period[df_meteo_full_period['DATE'].dt.year >= 1990]

# Fonction pour vérifier si une année est bissextile
def is_leap_year(year):
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

# Fonction pour calculer le 90ème percentile pour une station et une fenêtre de ±2 jours
def calculate_90_percentile(row, df_historique):
    day_of_year = row['DATE'].dayofyear
    station_id = row['NUM_POSTE']
    year = row['DATE'].year
    
    # Déterminer si l'année de la ligne est bissextile
    leap_year = is_leap_year(year)
    
    # Créer une liste de jours de l'année en tenant compte du changement d'année
    days_of_year_window = []
    for delta in range(-2, 3):
        adjusted_day = day_of_year + delta
        
        # Gestion du dépassement des jours dans l'année
        if adjusted_day < 1:
            adjusted_day += 366 if leap_year else 365
        elif adjusted_day > 366 and leap_year:
            adjusted_day -= 366
        elif adjusted_day > 365 and not leap_year:
            adjusted_day -= 365
        
        days_of_year_window.append(adjusted_day)
    
    # Extraire les données des mêmes jours ±2 dans l'année sur toutes les années historiques
    df_window = df_historique[
        (df_historique['NUM_POSTE'] == station_id) &
        (df_historique['DATE'].dt.dayofyear.isin(days_of_year_window))
    ]
    
    return df_window['TX'].quantile(0.9) if not df_window.empty else None



def calculate_percentile_90_10(group, l_col: list, f_quantile1: float, f_quantile2: float):
    percentiles = {}
    for i in range(365):
        day_of_year = i + 1

        if day_of_year == 1:
            window = group[(group['DATE'].dt.dayofyear >= 364) |
                           (group['DATE'].dt.dayofyear <= 3)]
        elif day_of_year == 2:
            window = group[(group['DATE'].dt.dayofyear >= 365) |
                           (group['DATE'].dt.dayofyear <= 4)]
        elif day_of_year >= 364:
            window = group[(group['DATE'].dt.dayofyear >= (day_of_year - 2)) |
                           (group['DATE'].dt.dayofyear <= (day_of_year - 364 + 2))]
        else:
            window = group[(group['DATE'].dt.dayofyear >= (day_of_year - 2)) &
                           (group['DATE'].dt.dayofyear <= (day_of_year + 2))]

        day_percentiles = {}
        for col in l_col:
            percentile_1 = window[col].quantile(f_quantile1)
            percentile_2 = window[col].quantile(f_quantile2)
            day_percentiles[f'{col}{format(f_quantile1*100, ".0f")}'] = percentile_1
            day_percentiles[f'{col}{format(f_quantile2*100, ".0f")}'] = percentile_2

        percentiles[day_of_year] = day_percentiles

    df_percentiles = pd.DataFrame.from_dict(percentiles, orient='index')
    return df_percentiles



# # Sélectionner une ligne spécifique du dataframe
# ligne_exemple = df_1990_onwards.iloc[364*11]  # par exemple, la première ligne
# specific_date = pd.to_datetime("1992-01-01")

# # Filtrage du DataFrame avec les conditions spécifiées
# ligne_exemple = df_meteo_full_period[(df_meteo_full_period["DATE"] == specific_date) & (df_meteo_full_period["NUM_POSTE"] == 1)].iloc[0]


# # Appliquer la fonction calculate_90_percentile à cette ligne
# percentile_90 = calculate_90_percentile(ligne_exemple, df_historique)

df_test = df_historique.groupby('NUM_POSTE').apply(calculate_percentile_90_10,
                                                  l_col=['TX','TN'],
                                                  f_quantile1=0.9, 
                                                  f_quantile2=0.1)



