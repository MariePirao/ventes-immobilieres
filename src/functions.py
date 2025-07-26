import os
import numpy as np
import pandas as pd
import requests
import time
import constantes as const
import requests
import zipfile
import io
from shapely.geometry import shape, mapping
from shapely import wkt
import geopandas as gpd
from shapely.geometry import Point
import json

def uplaodFiles():
    # ure de téléchargement des fichiers
    urls = {
        "https://www.data.gouv.fr/fr/datasets/r/0d16005c-f68e-487c-811b-0deddba0c3f1",
        "https://www.data.gouv.fr/fr/datasets/r/3942b268-04e7-4202-b96d-93b9ef6254d6",
        "https://www.data.gouv.fr/fr/datasets/r/b4f43708-c5a8-4f30-80dc-7adfa1265d74",
        "https://www.data.gouv.fr/fr/datasets/r/bc213c7c-c4d4-4385-bf1f-719573d39e90",
        "https://www.data.gouv.fr/fr/datasets/r/5ffa8553-0e8f-4622-add9-5c0b593ca1f8"
    }

    mutation_file = []  # liste pour stocker les morceaux filtrés

    for url in urls:
        response = requests.get(url)
        if response.status_code == const.DISTANCE_BOIS:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                nom_fichier = z.namelist()[0]
                with z.open(nom_fichier) as fichier:
                    chunks = pd.read_csv(fichier, sep='|', dtype=str, low_memory=False, chunksize=100_000)
                    for chunk in chunks:
                        chunk_filtered = chunk[chunk.iloc[:, 18] == '75']
                        mutation_file.append(chunk_filtered)
                        #taille = asizeof.asizeof(chunk_filtered)
                        #print(f"Lignes filtrées dans chunk: {len(chunk_filtered)}, taille approx. : {taille/(1024**2):.2f} Mo")
        else:
            print(f"Erreur {response.status_code} lors du téléchargement de {url}")

    # Concaténation des chunks
    return pd.concat(mutation_file, ignore_index=True) 


def remplir_code_postal(row):
    if pd.isna(row['Code postal']) and isinstance(row['Commune'], str) and "PARIS" in row['Commune']:
        try:
            arr = int(row['Commune'].split()[-1])
            if 1 <= arr <= 20:
                return f"750{arr:02d}"  # format : 75001, 75020...
        except:
            return np.nan
    return row['Code postal']


# Fonction qui complete les type de voie utile pour les coordonnées GPS
def completer_type_de_voie_cp(ventes):
    """
    Complète les valeurs manquantes dans la colonne 'Type de voie' et code_postal
    en utilisant les valeurs correspondantes de 'Code voie' à partir des lignes non nulles.

    Parameters:
    ventes (DataFrame) : DataFrame contenant les colonnes 'Code voie' et 'Type de voie' et code postal
    
    Returns:
    DataFrame mis à jour avec les valeurs manquantes dans 'Type de voie' et cdde postal remplies.
    """
    df_work = ventes.copy()
    # Créer un dictionnaire où les clés sont les 'Code voie' et les valeurs les 'Type de voie' non nulles
    code_voie_to_type = df_work[df_work['Type de voie'].notnull()][['Code voie', 'Type de voie']]
    code_voie_to_type = code_voie_to_type.drop_duplicates(subset='Code voie')
    correspondances_manuelles = {
        '9936': 'VLA',
        '4648': 'VLA',
        '0507': 'VLA',
        '6539': 'VLA',
        '7433': 'VLA',
        'X421': 'VOIE',
        'X670': 'VOIE',
        '8512': 'RPT',
        '7616': 'RPT',
        'R072': 'ALL'
    }
    # Fusionner les deux dictionnaires
    code_voie_to_type_manual = pd.DataFrame(list(correspondances_manuelles.items()), columns=['Code voie', 'Type de voie'])
    code_voie_to_type = pd.concat([code_voie_to_type, code_voie_to_type_manual]).drop_duplicates(subset='Code voie')

    df_work['Type de voie'] = df_work['Type de voie'].fillna(df_work['Code voie'].map(code_voie_to_type.set_index('Code voie')['Type de voie']))

    # 4️⃣ Remplacements explicites (pas de boucle)
    df_work.loc[df_work['Voie'] == 'VIL WAGRAM SAINT HONORE', 'Voie'] = 'WAGRAM SAINT HONORE'
    df_work.loc[df_work['Voie'] == 'VLA HONORE GABRIEL RIQUETI', 'Voie'] = 'HONORE GABRIEL RIQUETI'
    df_work.loc[df_work['Voie'] == 'VIL DE L ASTROLABE', 'Voie'] = 'DE L ASTROLABE'
    df_work.loc[df_work['Voie'] == 'VIL DU MONT TONNERRE', 'Voie'] = 'DU MONT TONNERRE'
    df_work.loc[df_work['Voie'] == 'VIL PIERRE GINIER', 'Voie'] = 'PIERRE GINIER'
    df_work.loc[df_work['Voie'] == 'ALLEE DES HORTENSIAS', 'Voie'] = 'DES HORTENSIAS'
    df_work.loc[df_work['Voie'] == 'R-PTSAINT CHARLES', 'Voie'] = 'SAINT CHARLES'

    df_work['Code postal'] = df_work.apply(remplir_code_postal, axis=1)

    return df_work



# Fonction qui renvoie vrai si le "groupe composant la vente conteint 1 et seul appartement et rien d'auters que des dépendances
def est_vente_valide(groupe):
    types = groupe["Type local"].dropna().unique()
    n_appart = (groupe["Type local"] == "Appartement").sum()
    return (
        n_appart == 1 and
        all(t in ["Appartement", "Dépendance"] for t in types)
    )

def createDfBySales(df):
    df_work= df.copy()
    # On calcule le nb de dépendances par vente_id  
    nb_dependances = df_work[df_work["Type local"] == "Dépendance"].groupby("vente_id").size().rename("nb_dependances")
    
    # On refait un dataframe avec uniquement les lignes appartement
    df_return = df_work[df_work["Type local"] == "Appartement"].copy()

    # on ajoutant le nombre de dépendance sur chaque ligne sur vente_id
    df_return = df_return.merge(nb_dependances, how="left", left_on="vente_id", right_index=True)

    # On aura Nan si on a pas de lignes de dépendances sur une vente donc il faut rempalcer par 0
    df_return["nb_dependances"] = df_return["nb_dependances"].fillna(0).astype(int)

    return df_return



def getCoordsFromAddress(address):
    url = "https://api-adresse.data.gouv.fr/search/"
    params = {'q': address}

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()  # Stoppe si code HTTP ≠ 200
        data = response.json()
        if data['features']:
            lon, lat = data['features'][0]['geometry']['coordinates']
            return round(lat, 5), round(lon, 5)
        else:
            print(f"Adresse non trouvée {address} ")
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Erreur de requête  {address} : {e}")
        return None, None
    except ValueError as e:
        print(f"Erreur de décodage JSON pour  {address} : {e}")
        return None, None
    
# Fonction qui ajoute les coordonnées GPS
def addGPSCoord(df):
    df_work= df.copy()
    df_work['adresseGPS'] = df_work['No_voie'] + ' ' + df_work['Type de voie'] + ' ' + df_work['Voie'] + ', Paris ' + df_work['Code postal']

    cache_file = 'adresses_gps.csv'
    # Charger le cache existant ou en créer un vide
    if os.path.exists(cache_file):
        df_cache = pd.read_csv(cache_file)
    else:
        df_cache = pd.DataFrame(columns=['adresseGPS', 'latitude', 'longitude'])

   # Préparer un dictionnaire depuis le cache
    coords_map = {row['adresseGPS']: (row['latitude'], row['longitude']) for _, row in df_cache.iterrows()}

    # Identifier les adresses à traiter
    unique_addresses = df_work['adresseGPS'].unique()
    addresses_to_fetch = [addr for addr in unique_addresses if addr not in coords_map]

    print(f"Nombre d'adresses déjà en cache : {len(coords_map)}")
    print(f"Nombre d'adresses à interroger via API : {len(addresses_to_fetch)}")

    # Récupérer les coordonnées manquantes
    new_entries = []
    for i, address in enumerate(addresses_to_fetch):
        lat, lon = getCoordsFromAddress(address)
        if lat is not None and lon is not None:
            coords_map[address] = (lat, lon)
            new_entries.append({'adresseGPS': address, 'latitude': lat, 'longitude': lon})
        if i % 10 == 0 and i > 0:
            time.sleep(1)

    # Mettre à jour le cache si on a de nouvelles entrées
    if new_entries:
        df_new = pd.DataFrame(new_entries)
        df_cache = pd.concat([df_cache, df_new], ignore_index=True)
        df_cache.to_csv(cache_file, index=False)
        print(f"{len(new_entries)} nouvelles adresses ajoutées au cache.")

    # Associer les coordonnées au dataframe principal
    df_work['latitude'] = df_work['adresseGPS'].map(lambda x: coords_map.get(x, (None, None))[0])
    df_work['longitude'] = df_work['adresseGPS'].map(lambda x: coords_map.get(x, (None, None))[1])

    if not df_work[df_work['latitude'].isna() & df_work['longitude'].isna()].empty:
        print("\n❌ Adresses avec coordonnées manquantes (latitude et longitude) :")
        print(df_work[df_work['latitude'].isna() & df_work['longitude'].isna()][['adresseGPS']])

    # Nettoyer la colonne temporaire
    df_work = df_work.drop(['adresseGPS'], axis=1)

    return df_work
    
    
# Fonction qui corrige les nb de pièces par rapport au m2
def nbRoomCorrection(df):
    df_return = df.copy()

    # Calculer l’écart entre la surface attendues par rapport au nomb de pièce et la surface réelle
    df_return['ecart'] = (df_return['Nombre pieces principales'] * 9) - df_return['Surface reelle bati']

    # Identifier les lignes à corriger (écart > 10 m²)
    errors = df_return['ecart'] > 10

    #print("\nAvant correction :")
    #print(df_return.loc[errors, ['Surface reelle bati', 'Nombre pieces principales']])

    # Corriger le nombre de pièces en forcant à au moins1 pièce
    df_return.loc[errors, 'Nombre pieces principales'] = (df_return.loc[errors, 'Surface reelle bati'] // 9).astype(int)
    df_return.loc[df_return['Nombre pieces principales'] < 1, 'Nombre pieces principales'] = 1

    #print("\nAprès correction :")
    #print(df_return.loc[errors, ['Surface reelle bati', 'Nombre pieces principales']])

    df_return = df_return.drop([ 'ecart'], axis=1)

    return df_return


def calc_env_geo(df):
    """
    Ajoute une colonne 'ZoneElargie' au GeoDataFrame d'entrées,
    représentant une zone circulaire de DISTANCE_VENTE m autour de chaque point de vente.
    Les coordonnées doivent être en EPSG:4326 (WGS84).
    """
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs='EPSG:4326')

    # Reprojection en mètres (Web Mercator) pour pouvoir faire un buffer en mètres
    gdf_proj = gdf.to_crs(epsg=3857)

    # Calcul du buffer de 200 mètres
    gdf_proj['ZoneElargie'] = gdf_proj.geometry.buffer(const.DISTANCE_VENTE)

    # Reprojection éventuelle du buffer vers EPSG:4326 (si souhaitée dans un contexte cartographique mondial)
    gdf['ZoneElargie'] = gdf_proj['ZoneElargie'].to_crs(epsg=4326)

    return gdf

def ajouter_quartier(code_commune_section):
    code_commune = code_commune_section[:3]
    section = code_commune_section[-2:]
    
    for quartier, info in const.quartier_section_commune.items():
        if code_commune == info['commune'] and section in info['sections']:
            return quartier
    return None 

def add_id_Loyer(df):
    df_work = df.copy()
    #On créé les colonnes dont on a besoin pour le merge
    df_work['Quartier'] = df_work['SectionCadastraleID'].apply(ajouter_quartier)
    df_work['Annee'] = df_work['DateVente'].dt.year

    #On réupère le fichir des Loyer et on supprime les colonnes dont on a pas besoin
    df_Loyer = pd.read_csv('OutData/loyerReference.csv')
    df_Loyer = df_Loyer.drop(columns=["LoyerMoyenMeuble", "LoyerMoyenNonMeuble"])

    #ce merge permet de récupérer uniquement LoyerID
    df_return = df_work.merge(df_Loyer,on=['Annee', 'NombrePiece', 'Quartier'], how='left')
    
    df_return = df_return.drop(columns=["Annee","Quartier"])

    return df_return

def uplaodFilesBois():
    base_url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/espaces_verts/records?select=nsq_espace_vert%2C%20nom_ev%2C%20poly_area%2C%20geom%2C%20geom_x_y&where=categorie%20%3D%20%22Bois%22&limit=20"
    response = requests.get(base_url).json()
    df = pd.DataFrame(response['results'])
    print("Total d'enregistrements récupérés ", df.shape)

    return df

def uplaodFilesEV():
    base_url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/espaces_verts/records"
    limit = 100
    offset = 0
    all_results = []

    while True:
        params = {
            'limit': limit,
            'offset': offset
        }
        response = requests.get(base_url, params=params).json()

        results = response.get('results', [])
        if not results:
            break

        all_results.extend(results)
        offset += limit

    df = pd.DataFrame(all_results)
    # suppression des bois
    df = df[df['categorie'] != 'Bois'].copy()
    df = df[df['nsq_espace_vert'].notna()]
    df['nsq_espace_vert'] = df['nsq_espace_vert'].astype(float).astype(int).astype(str)
    print("Total d'enregistrements récupérés ", df.shape)

    return df


def areaEnlargement(df_bois):
    
    df_work = df_bois.copy()
    # On ne garde que "coordinates" de la variable geom
    df_work['geom'] = df_work['geom'].apply(lambda x: x['geometry'])
    
    # On convertit la colonne 'geom' en objets shapely.Geometry
    df_work['geometry'] = df_work['geom'].apply(
        lambda x: shape(json.loads(x)) if isinstance(x, str) else shape(x)
    )
    # On crée un GeoDataFrame en WGS84 (EPSG:4326) 
    df_work = gpd.GeoDataFrame(df_work, geometry='geometry', crs="EPSG:4326")

    # On Reprojeter en EPSG:2154 (Lambert 93 - mètres)
    df_work_2154 = df_work.to_crs(epsg=2154)

    # On créé le buffer de 200 mètres
    df_work_2154['ZoneElargie'] = df_work_2154['geometry'].buffer(1000)

    # On Reprojete les buffers en WGS84
    ZoneElargie_wgs84 = gpd.GeoSeries(
        df_work_2154['ZoneElargie'], crs="EPSG:2154").to_crs(epsg=4326)

    # On ajoute les buffers reprojetés au DataFrame d'origine
    df_work['ZoneElargie'] = ZoneElargie_wgs84

    # On convertit les buffers en string GeoJSON (Geo Shape compatible) 
    df_work['ZoneElargie'] = df_work['ZoneElargie'].apply(
        lambda geom: json.dumps(mapping(geom)) if geom else None
    )

    return df_work
    
def compute_area_corrected(geojson_dict):
    if not geojson_dict:
        return None
    geometry = geojson_dict.get('geometry')
    if not geometry:
        return None
    try:
        geom = shape(geometry)
        if not geom.is_valid:
            geom = geom.buffer(0)  # correction géométrique
        gdf = gpd.GeoSeries([geom], crs="EPSG:4326")
        gdf_proj = gdf.to_crs("EPSG:2154")
        return gdf_proj.area.iloc[0]
    except Exception as e:
        print(f"Erreur pour la géométrie {geometry} : {e}")
        return None
    
def parse_geometry(x):
    if x is None:
        return None
    elif isinstance(x, str):
        return shape(json.loads(x))
    else:
        return shape(x)


def areaEnlargementEV(df_EV):
    
    df_work = df_EV.copy()
    # On convertit la colonne 'geom' en objets shapely.Geometry
    df_work['geometry'] = df_work['geom'].apply(parse_geometry)
    # On crée un GeoDataFrame en WGS84 (EPSG:4326) 
    df_work = gpd.GeoDataFrame(df_work, geometry='geometry', crs="EPSG:4326")

    # On Reprojeter en EPSG:2154 (Lambert 93 - mètres)
    df_work_2154 = df_work.to_crs(epsg=2154)

    # On créé le buffer de 200 mètres
    df_work_2154['ZoneElargie'] = df_work_2154['geometry'].buffer(200)

    # On Reprojete les buffers en WGS84
    ZoneElargie_wgs84 = gpd.GeoSeries(
        df_work_2154['ZoneElargie'], crs="EPSG:2154").to_crs(epsg=4326)

    # On ajoute les buffers reprojetés au DataFrame d'origine
    df_work['ZoneElargie'] = ZoneElargie_wgs84

    # On convertit les buffers en string GeoJSON (Geo Shape compatible) 
    df_work['ZoneElargie'] = df_work['ZoneElargie'].apply(
        lambda geom: json.dumps(mapping(geom)) if geom else None
    )

    return df_work
        

def convGeoVente(df_ventes): 
    ventes = df_ventes.copy()
    gdf_ventes = gpd.GeoDataFrame(ventes, geometry=gpd.points_from_xy(ventes['Longitude'], ventes['Latitude']),crs="EPSG:4326")
    return gdf_ventes

# Conversion sécurisée de la colonne 'geom' en géométrie
def safe_parse_geometry(g):
    if g and 'geometry' in g:
        return shape(g['geometry'])
    else:
        return None

def convGeoEV(EV): 
    association = EV.copy()
    association['geometry'] = association['ZoneElargie'].apply(lambda z: shape(json.loads(z)))
    gdf_EV = gpd.GeoDataFrame(association, geometry='geometry', crs="EPSG:4326")
    return gdf_EV

def spatialJoin(gdf_ev, gdf_ventes): 
    join = gpd.sjoin(gdf_ventes, gdf_ev, how='inner', predicate='within')
    colonnes_a_garder = ['VenteID', 'EspaceVertID']
    join_clean = join[colonnes_a_garder].dropna()
    return join_clean

def uplaodFilesRisque():

    # Appel de l'API
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/plu-secteurs-de-risques-delimites-par-le-ppri/records?select=zonage%2C%20n_sq_pprizone%2C%20geo_shape"

    limit = 100
    offset = 0
    all_results = []
    while True:
        params = {
            'limit': limit,
            'offset': offset
        }
        response = requests.get(url, params=params).json()
        results = response.get('results', [])
        if not results:
            break
        all_results.extend(results)
        offset += limit
    # Récupération des données au format csv :
    df_zr = pd.DataFrame(all_results)
    print(f"Total lignes récupérées : {len(df_zr)}")  

    return df_zr

def convertionGeoShape(df_zr):

    df = df_zr.copy()

    # Conversion de 'geo_shape' en géométries Shapely :
    df['geometry'] = df['geo_shape'].apply(lambda x: shape(x['geometry']) if 'geometry' in x else shape(x))

    # Création du GeoDataFrame pour les zones risque :
    gdf_risque = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')

    return gdf_risque


def add_id_Risque(df):
    ''' Jointure spatiale des deux GeoDataFrames : 
    si une vente est dans une zone à risque, alors ZoneRisqueID est rempli (ex: B_216) 
    ; sinon : ZoneRisqueID est NaN.
    '''
    # Création du GeoDataFrame pour les ventes :
    gdf_ventes = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326")

    #On réupère le fichir des Loyer et on supprime les colonnes dont on a pas besoin
    df_zr = pd.read_csv('OutData/zone_risque.csv')
    df_zr['geometry'] = df_zr['GeoZone'].apply(wkt.loads)  # <-- conversion WKT → Shapely

    # Création du GeoDataFrame pour les zones de risque :
    gdf_risque = gpd.GeoDataFrame(df_zr[['ZoneRisqueID', 'geometry']], geometry='geometry', crs='EPSG:4326')


    gdf_ventes_zr = gpd.sjoin(gdf_ventes, gdf_risque, how='left', predicate='within')
    gdf_ventes_zr = gdf_ventes_zr.drop(columns=["geometry", "index_right"])

    return gdf_ventes_zr    

def add_id_Bois(df):
    ''' Jointure spatiale des deux GeoDataFrames : 
    si une vente est près d'une zone de bois, alors BoisId est rempli
    ; sinon : BoisId est NaN.
    '''
    # Création du GeoDataFrame pour les ventes :
    gdf_ventes = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326")

    #On réupère le fichir des Loyer et on supprime les colonnes dont on a pas besoin
    df_bois = pd.read_csv('OutData/bois.csv')
    df_bois['geometry'] = df_bois['ZoneElargie'].apply(lambda z: shape(json.loads(z)))
    df_bois['BoisID'] = df_bois['BoisID'].astype(object)
    df_bois = df_bois.drop(columns=["NomBois", "SurfaceBois", "ZoneElargie"])
 
    # Création du GeoDataFrame pour les zones de risque :
    gdf_bois = gpd.GeoDataFrame(df_bois, geometry='geometry', crs='EPSG:4326')


    gdf_ventes_bois = gpd.sjoin(gdf_ventes, gdf_bois, how='left', predicate='within')
    gdf_ventes_bois = gdf_ventes_bois.drop(columns=["geometry", "index_right"])
 
    return gdf_ventes_bois    


'''
ETABLISSEMENT SCOLAIRE
'''
def uplaodFilesEtablissementsScolaires():

        # Appel de l'API :
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/etablissements-scolaires-maternelles/records?select=libelle%2C%20geo_point_2d"

    limit = 100
    offset = 0
    all_results = []
    while True:
        params = {
            'limit': limit,
            'offset': offset
        }
        response = requests.get(url, params=params).json()
        results = response.get('results', [])
        if not results:
            break
        all_results.extend(results)
        offset += limit

    # Récupération des données au format csv :
    df_ets = pd.DataFrame(all_results)
    print(f"Total lignes récupérées : {len(df_ets)}")  

    return df_ets