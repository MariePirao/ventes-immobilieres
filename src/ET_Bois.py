import pandas as pd
import functions as funct
import constantes as const

df_bois = funct.uplaodFilesBois()

# Création d'une zone de 200 mètres autour du Geo Shape
df_bois = funct.areaEnlargement(df_bois)

# Retrait de la colonne geometry et Geopoint original
df_bois = df_bois.drop(['geom','geometry','geom_x_y'], axis=1)

#renomage des colonnes
df_bois = df_bois.rename(columns=const.COLUMN_RENAME_BOIS)
df_bois['BoisID'] = df_bois['BoisID'].astype(object)
df_bois = df_bois.reset_index(drop=True)
print(df_bois.info())

# Sauvegarde
df_bois.to_csv("OutData/bois.csv", index=False)
print(f"Fichier créé : {len(df_bois)} bois ")

