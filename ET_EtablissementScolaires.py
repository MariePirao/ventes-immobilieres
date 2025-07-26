import functions as funct
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd

df_ets = funct.uplaodFilesEtablissementsScolaires()

# Tri par libellé d'établissement :
df_ets = df_ets.sort_values(by = 'libelle', ascending = True)

# Création des variables "Latitude" et "Longitude" et suppression de la variable 'geo_point_2d' désormais inutile :
df_ets['Latitude'] = df_ets['geo_point_2d'].apply(lambda x: x['lat']).round(6)
df_ets['Longitude'] = df_ets['geo_point_2d'].apply(lambda x: x['lon']).round(6)
df_ets = df_ets.drop('geo_point_2d', axis = 1)

# Suppression des doublons :
# D'abord au niveau des 'Latitude' et 'Longitude'
df_ets = df_ets.drop_duplicates(subset=['Latitude', 'Longitude'])

# Suppression des doublons résiduels au niveau du libellé de l'établissement
df_ets = df_ets.drop_duplicates(subset='libelle')

# Reset de l'index :
df_ets = df_ets.reset_index()
df_ets = df_ets.drop('index', axis = 1)

# Création de l'EtablissementID :
df_ets['EtablissementID'] = 'ETS_' + df_ets.index.astype(str)
df_final_ets = df_ets[['EtablissementID', 'libelle', 'Latitude', 'Longitude']]
df_final_ets = df_final_ets.rename({'libelle': 'NomEtablissement'}, axis = 1)
df_final_ets.to_csv("OutData/etablissement_scolaire.csv", index=False)



# VENTE - ETABLISSEMENTS SCOLAIRE

# Création du GeoDataFrame pour les ventes :
df_ventes = pd.read_csv("OutData/ventes.csv", sep=",")
gdf_ventes = gpd.GeoDataFrame(df_ventes, geometry=gpd.points_from_xy(df_ventes.Longitude, df_ventes.Latitude), crs='EPSG:4326')

# Création du GeoDataFrame pour les établissements scolaires :
df_ets['geometry'] = df_ets.apply(lambda row: Point(row['Longitude'], row['Latitude']), axis=1)
gdf_ets = gpd.GeoDataFrame(df_ets, geometry='geometry', crs='EPSG:4326')

# Projection des données en mètres (pour pouvoir créer le buffer) :
gdf_ventes = gdf_ventes.to_crs(epsg=3857)
gdf_ets = gdf_ets.to_crs(epsg=3857)

# Création d'un buffer de 200m (= zone circulaire) autour de chaque vente :
gdf_ventes['geometry_buffer'] = gdf_ventes.geometry.buffer(200)

# Jointure spatiale pour trouver les établissements dans chaque buffer :
# On doit d'abord créer un GeoDataFrame :
gdf_ventes_buffer = gdf_ventes[['VenteID', 'geometry_buffer']].copy()
gdf_ventes_buffer = gdf_ventes_buffer.set_geometry('geometry_buffer')

# Jointure spatiale des deux GeoDataFrames : affiche l'Etablissement ID pour chaque VenteId (plusieurs fois le même VenteID si plusieurs étbalissements dans la zone de 200m autour de la vente) :
gdf_join = gpd.sjoin(gdf_ventes_buffer, gdf_ets[['EtablissementID', 'geometry']], how='inner', predicate='contains')

# Création de la table d'association Vente_etablissement à partir du GeoDataFrame gdf_join :
df_vente_etablissement = gdf_join[['VenteID', 'EtablissementID']].drop_duplicates()

# Export en csv :
df_vente_etablissement.to_csv("OutData/vente_etablissement.csv", index=False)