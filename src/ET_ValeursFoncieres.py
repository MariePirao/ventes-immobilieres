import pandas as pd
import functions as funct
import constantes as const

pd.set_option('display.max_columns', None)

mutations = funct.uplaodFiles()
#print(mutations.shape)  # (389586, 43)

#On filtre sur les ventes filtrage sur ventes   
ventes = mutations[mutations["Nature mutation"] == "Vente"]
#On supprime les lignes sur lesquelles on ne pourra pas faire d'ID
ventes = ventes.dropna(subset=["Date mutation", "Valeur fonciere"])
#print(ventes.shape)      #(377825, 43) 

#concaténation de deux colonnes
ventes['No_voie'] = ventes['No voie'] + ventes['B/T/Q'].fillna('')
#calcule de la futur clé secondaire de la table section cadastrale
ventes['CodeCommune_section'] = ventes['Code commune'] + ventes['Section']
#drop de colonnes non utile pour le projet
ventes = ventes.drop(['No disposition','Nature mutation','Identifiant de document', 'Reference document', '1 Articles CGI', '2 Articles CGI', '3 Articles CGI', '4 Articles CGI', 
                      '5 Articles CGI', 'No voie', 'B/T/Q', 'Prefixe de section', 'No Volume', '1er lot', 'Surface Carrez du 1er lot', '2eme lot', 'Surface Carrez du 2eme lot',
                      '3eme lot', 'Surface Carrez du 3eme lot', '4eme lot', 'Surface Carrez du 4eme lot', '5eme lot', 'Surface Carrez du 5eme lot', 'Identifiant local',
                      'Nature culture', 'Nature culture speciale', 'Surface terrain', 'Code departement','No plan','Nombre de lots','Code type local','Code commune','Section'], axis=1)

# On a vu qu'un même dependance était en doublon par rapport a la colonne Nature culture
# comme on a supprimé cette colonne on doit dedupliquer les lignes
ventes = ventes.drop_duplicates()

# Nettoyage et Conversion "Valeur fonciere" en float et autres convertions
ventes['Valeur fonciere'] = ventes['Valeur fonciere'].str.replace(' ', '').str.replace(',', '.').astype(float)
ventes['Surface reelle bati'] = pd.to_numeric(ventes['Surface reelle bati'], errors='coerce')
ventes['Nombre pieces principales'] = pd.to_numeric(ventes['Nombre pieces principales'], errors='coerce').astype('Int64')
ventes['Date mutation'] = pd.to_datetime(ventes['Date mutation'], dayfirst=True, errors='coerce')
#print(ventes.shape)      #(316331, 12)  

#creation nouvelle colonne id 
ventes['DateMutation_str'] = ventes['Date mutation'].astype(str).str.strip()
ventes['ValeurFonciere_str'] = ventes['Valeur fonciere'].astype(int).astype(str)
ventes['vente_id'] = ventes['DateMutation_str'] + "_" + ventes['ValeurFonciere_str']
ventes = ventes.drop(['DateMutation_str', 'ValeurFonciere_str'], axis=1)
#print(ventes.shape) #(316331, 13)

# On créé un nouveau dataframe ne contenant que les ventes avec un et un seul appartement et 0 ou N dépendances
ventes_valides = ventes.groupby("vente_id").filter(funct.est_vente_valide)
#print(ventes_valides.shape)  #(194538, 13)

#On construit un dataframe en utilisant l'id et en supprimant les lignes de dépendance mais en gardant le nombre
ventes_appart = funct.createDfBySales(ventes_valides)
#on drop les données dont on ne se sert plus
ventes_appart = ventes_appart.drop('Type local', axis=1)
#on supprime les 4 lignes où nous n'avons pas de surface ni de nombre de pièces 
ventes_appart = ventes_appart.dropna(subset=['Surface reelle bati', 'Nombre pieces principales'])
#print(ventes_appart.shape)  #(123998, 13)

# Ajouter des coordonnées GPS des adresses des appartements
ventes_appart = funct.completer_type_de_voie_cp(ventes_appart)
ventes_appart = ventes_appart.drop('Code voie', axis=1)
ventes_appart = funct.addGPSCoord(ventes_appart)
#print(ventes_appart.shape)  #(123998, 14)

#on nettoie les données entre m2 et nombres de pièces
ventes_appart = funct.nbRoomCorrection(ventes_appart)
#print(ventes_appart.shape)  #(123998, 14)

#On calcule une zone autour de la vente 
ventes_appart = funct.calc_env_geo(ventes_appart)
#print(ventes_appart.shape)  #(123998, 18)

#renomage des colonnes
ventes_appart = ventes_appart.rename(columns=const.COLUMN_RENAME_VENTE)
#print(ventes_appart.shape) #(123998, 18)

#on ajoute les clés secondaire LoyerId/
ventes_appart = funct.add_id_Loyer(ventes_appart)

#on ajoute les clés secondaire risqueID/
ventes_appart = funct.add_id_Risque(ventes_appart)

#on ajoute les clés secondaire boisID/
ventes_appart = funct.add_id_Bois(ventes_appart)
print(ventes_appart.info())

# Sauvegarde
ventes_appart.to_csv("OutData/ventes.csv", index=False)
print(f"Fichier créé : {len(ventes_appart)} ventes valides (1 appart, 0+ dépendance(s))")