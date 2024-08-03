import numpy as np
import pandas as pd

def is_leap_year(year):
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

# Fonction pour calculer le 90ème percentile pour une station et une fenêtre de ±2 jours
def calculate_90_percentile1(row, df_historique):
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


def calculate_percentile(group, s_col:str, f_quantile:float):
    percentiles = {}
    for i in range(365):
        # Jour cible
        day_of_year = i + 1

        # Définir la fenêtre de 5 jours
        if day_of_year == 1:
            # Inclure les derniers jours de l'année précédente
            window = group[(group['DATE'].dt.dayofyear >= 364) | 
                           (group['DATE'].dt.dayofyear <= 3)]
        elif day_of_year == 2:
            # Inclure les derniers jours de l'année précédente et les premiers jours de l'année
            window = group[(group['DATE'].dt.dayofyear >= 365) | 
                           (group['DATE'].dt.dayofyear <= 4)]
        elif day_of_year >= 364:
            # Inclure les premiers jours de l'année suivante
            window = group[(group['DATE'].dt.dayofyear >= (day_of_year - 2)) | 
                           (group['DATE'].dt.dayofyear <= (day_of_year - 364 + 2))]
        else:
            # Fenêtre classique pour les autres jours
            window = group[(group['DATE'].dt.dayofyear >= (day_of_year - 2)) & 
                           (group['DATE'].dt.dayofyear <= (day_of_year + 2))]

        # Calculer le 90ème percentile
        percentile_90 = window[s_col].quantile(f_quantile)
        percentiles[day_of_year] = percentile_90
    
    return pd.Series(percentiles)




def compute_percentile(group, l_col: list, f_quantile1: float, 
                       f_quantile2: float):
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
    
    df_percentiles = df_percentiles.reset_index()
    df_percentiles=df_percentiles.rename(columns={'level_1':'JOUR_ANNEE'})
    
    
    return df_percentiles