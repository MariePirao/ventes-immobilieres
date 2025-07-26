import pandas as pd
import requests
import constantes as const # un fichier pour avoir au même endroit les colonnes si on veut les renommer facilement

# appel de l'API avec une condition de regroupement
url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/logement-encadrement-des-loyers/records?select=annee%2C%20nom_quartier%2C%20piece%2C%20meuble_txt%2C%20avg(ref)%20as%20LoyerMoyen&group_by=annee%2C%20nom_quartier%2C%20piece%2C%20meuble_txt"

response = requests.get(url)

# On regarde si l'appel a fonctionné
if response.status_code == 200:
    data = response.json()
    records = data['results']
    # Création du dataframe
    encadrement = pd.DataFrame(records)

#print(encadrement.shape) #= (3840, 5)

# Conversion de la colonne de loyers en numérique
encadrement['LoyerMoyen'] = pd.to_numeric(encadrement['LoyerMoyen'], errors='coerce')
#on doit modifier certines noms de quartier car ils sont mal orthographié
encadrement['nom_quartier'] = encadrement['nom_quartier'].replace(const.REMPLACEMENTS_QUARTIER)

# On créé un dataframe de lignes de meublé et on renomme la colonne LoyerMoyen en LoyerMoyenMeuble et on supprime la colonne meuble_txt
df_meuble = encadrement[encadrement['meuble_txt'] == 'meublé'][['annee', 'nom_quartier', 'piece', 'LoyerMoyen']]
df_meuble = df_meuble.drop(columns=['meuble_txt'], errors='ignore')
df_meuble = df_meuble.rename(columns={'LoyerMoyen': 'LoyerMoyenMeuble'})
df_meuble['LoyerMoyenMeuble'] = df_meuble['LoyerMoyenMeuble'].round(2)

# On créé un dataframe de lignes de non meublé et on renomme la colonne LoyerMoyen en LoyerMoyenNonMeuble et on supprime la colonne meuble_txt
df_non_meuble = encadrement[encadrement['meuble_txt'] == 'non meublé'][['annee', 'nom_quartier', 'piece', 'LoyerMoyen']]
df_non_meuble = df_non_meuble.drop(columns=['meuble_txt'], errors='ignore')
df_non_meuble = df_non_meuble.rename(columns={'LoyerMoyen': 'LoyerMoyenNonMeuble'})
df_non_meuble['LoyerMoyenNonMeuble'] = df_non_meuble['LoyerMoyenNonMeuble'].round(2)

# Maintenent si on merge avec les colonnes 'annee', 'nom_quartier', 'piece' on va de cette facon regrouper les lignes 
# et donc les deux colonnes LoyerMoyenNonMeuble et LoyerMoyenMeuble vont être concervés
encadrement_agg = pd.merge(df_meuble, df_non_meuble, on=['annee', 'nom_quartier', 'piece'])

# On créé l'ID par concaténation  
encadrement_agg['LoyerID'] = encadrement_agg['annee'].astype(str) + encadrement_agg['nom_quartier'] + encadrement_agg['piece'].astype(str)

#On renomme pour être conforme a la table
encadrement_agg = encadrement_agg.rename(columns=const.COLUMN_RENAME_LOYER)

#print(encadrement_agg.shape) #= (3840, 9)

# Creation des CSV pour insertion dans les tables
encadrement_agg.to_csv('OutData/loyerReference.csv', index=False)
print(f"Fichier créé : {len(encadrement_agg)} loyer de référence ")