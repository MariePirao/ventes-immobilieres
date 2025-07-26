import functions as funct


df_zr = funct.uplaodFilesRisque()

# Création de la clé primaire ZoneRisqueID à partir de 'zonage' et de 'n_sq_pprizone' :
df_zr['ZoneRisqueID'] = df_zr['zonage'].astype(str) + "_" + df_zr['n_sq_pprizone'].astype(str)

# Création du GeoDataFrame pour les zones risque :
gdf_risque = funct.convertionGeoShape(df_zr)

# Création du csv ZoneRisque et export :
df_final_zr = gdf_risque[['ZoneRisqueID', 'zonage', 'geometry']].drop_duplicates()
df_final_zr = df_final_zr.rename({'zonage':'IntensiteRisque', 'geometry':'GeoZone'}, axis = 1)

#creation du csv des risques
df_final_zr.to_csv("OutData/zone_risque.csv", index=False)
print(f"Fichier créé : {len(df_final_zr)} risques")



