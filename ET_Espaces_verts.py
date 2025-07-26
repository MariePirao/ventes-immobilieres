import pandas as pd
import functions as funct
import constantes as const

df_EV = funct.uplaodFilesEV()

# Nettoyage pour les deux futurs dataframes de toutes les colonnes inutiles dans le projet
df_EV = df_EV.drop(['last_edited_user','last_edited_date','presence_cloture','annee_ouverture',
                                      'annee_renovation','ancien_nom_ev','annee_changement_nom','nb_entites','url_plan',
                                      'ouvert_ferme','id_atelier_horticole','id_division','ida3d_enb','site_villes',
                                      'id_eqpt','competence','perimeter','surface_horticole', 'surface_totale_reelle','adresse_libellevoie',
                                      'adresse_typevoie','adresse_complement','adresse_numero'], axis=1)

# recalcul de la surface en m2 pour selectionner les esapces verts ayant un interet pour l'analyse
df_EV['surface_m2_corrected'] = df_EV['geom'].apply(funct.compute_area_corrected)
# On remplace les NaN de poly_area avec la surface que nous avons calculée grâce au GeoShape
df_EV["poly_area"] = df_EV["poly_area"].fillna(df_EV["surface_m2_corrected"]).round(0)

# Retirer toutes les lignes dont la surface_m2 est inférieure à 2000 m2
df_EV = df_EV[df_EV["poly_area"] >= 2000]

df_EV = funct.areaEnlargementEV(df_EV)

# Retrait des colonnes inutiles 
df_EV = df_EV.drop(['surface_m2_corrected','geom','geom_x_y','geometry'], axis=1)

#renomage des colonnes
df_EV = df_EV.rename(columns=const.COLUMN_RENAME_EV)
df_EV = df_EV.reset_index(drop=True)

#conversion
df_EV['EspaceVertID'] = df_EV['EspaceVertID'].astype(object)
print(df_EV.info())

# Sauvegarde
df_EV.to_csv("OutData/espacesVerts.csv", index=False)
print(f"Fichier créé : {len(df_EV)} esapces verts")


# CREATION TABLE ASSOCIATION VENTE-ESPACE VERT
chemin = "OutData/ventes.csv"
df_ventes = pd.read_csv(chemin, sep=',')

df_ventes = df_ventes.drop(['DateVente', 'ValeurFonciere', 'CodePostal', 'Commune', 'SurfaceReelle', 
                            'NombrePiece', 'NumeroVoie', 'SectionCadastraleID', 'NombreDependance', 
                            'LoyerID','ZoneRisqueID','BoisID','TypeVoie','Voie','ZoneElargie'], axis=1)
print(df_ventes.info())

df_EV = df_EV.drop(['TypeEspaceVert', 'CodePostal', 'CategorieEspaceVert', 'SurfaceEspaceVert','NomEspaceVert'], axis=1)
print(df_EV.info())

# Convertir les ventes en points géographiques
gdf_ventes = funct.convGeoVente(df_ventes)

# Conversion sécurisée de la colonne 'geom' en géométrie
gdf_ev = funct.convGeoEV(df_EV)

#exécuter une jointure spatial
vente_EspaceVert = funct.spatialJoin(gdf_ev, gdf_ventes)
print(vente_EspaceVert.info())

# Sauvegarde
vente_EspaceVert.to_csv("OutData/vente_EspaceVert.csv", index=False)
print(f"Fichier créé : {len(vente_EspaceVert)} ventes/espaces verts")
