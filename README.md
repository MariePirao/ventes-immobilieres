# 🏘️ Analyse des ventes immobilières parisiennes

Ce projet avait pour objectif d’identifier les facteurs influençant les ventes immobilières à Paris, à partir de données open data enrichies.
Donnée vente : téléchargement des fichiers DVF
Données externes : par API sur Open data

## 🎯 Objectifs
- Centraliser et structurer les données DVF (Demandes de Valeurs Foncières)
- Enrichir les ventes avec des données géographiques : écoles, espaces verts, zones à risques, encadrement des loyers…
- Construire un datawarehouse modélisé en flocon
- Créer un rapport Power BI interactif pour visualiser les impacts de ces facteurs

## 🔧 Méthodologie
- Extraction de données (via fichiers CSV et API)
- Transformation et nettoyage sous Python (Pandas, GeoPandas)
- Enrichissement spatial (coordonnées, zones d’intérêt)
- Chargement dans un modèle relationnel (PostgreSQL)
- Visualisation finale via Power BI

## 📎 Accès aux fichiers

- 📁 Rapport Power BI (.pbix) → [Télécharger via Google Drive](https://drive.google.com/file/d/1AUfRecW9EAl9ih1BDsMU84pW0jWr-Qu6/view?usp=drive_link)  
- 📁 Données sources  → [Télécharger via google drive](https://drive.google.com/drive/folders/1Xk3dAhaE9kh_IQRuOBfOWyDp6Z1XhMrx?usp=drive_link)

## 📷 Aperçu

![Aperçu du rapport Power BI](./captures/dashboard.png)

## 🛠️ Technologies utilisées
- Python (Pandas, GeoPandas)
- SQL (PostgreSQL, pgAdmin)
- Power BI (DAX, Power Query)

## 📈 Exemples de visualisations
- Évolution du prix au m² selon les arrondissements
- Impact de la proximité des écoles ou espaces verts
- Analyse du rendement locatif selon la typologie des biens

---

**Projet réalisé en équipe dans le cadre du module ETL / Data Visualisation.**
