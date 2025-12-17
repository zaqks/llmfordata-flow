# 1. IMPORTATION DES OUTILS
import requests
from bs4 import BeautifulSoup
import warnings

# On ignore les avertissements XML/HTML pour garder la console propre
warnings.filterwarnings("ignore", category=UserWarning)

def scrapper_databricks_rss():
    url = "https://www.databricks.com/feed"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    print("Récupération du flux RSS Databricks en cours...")
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        # On utilise le parser XML (lxml) si dispo, sinon html.parser
        # Note: html.parser transforme les balises en minuscules
        soup = BeautifulSoup(response.text, 'html.parser') 
        
        # ---------------------------------------------------------
        # ATTENTION : Tout ce qui touche au fichier "f" doit être 
        # indenté (décalé) sous le "with open" !
        # ---------------------------------------------------------
        with open("donnees_databricks.txt", "w", encoding="utf-8") as f:
            f.write("=== BLOG DATABRICKS (VIA RSS) ===\n\n")
            
            items = soup.find_all("item")
            
            for item in items[:10]:
                # TITRE
                titre = item.find("title").text if item.find("title") else "Pas de titre"
                
                # DATE
                date_pub = item.find("pubdate").text if item.find("pubdate") else "Date inconnue"
                
                # LIEN (Correction ici : on utilise 'guid' au lieu de 'link')
                # guid contient souvent l'URL dans les flux RSS Databricks
                lien = item.find("guid").text if item.find("guid") else "Pas de lien"
                
                # ÉCRITURE (Doit être aligné sous le 'for')
                f.write(f"Titre : {titre}\n")
                f.write(f"Date  : {date_pub}\n")
                f.write(f"Lien  : {lien}\n")
                f.write("-" * 50 + "\n")
                
        # Ici on est revenu à gauche, le fichier est fermé automatiquement.
        print("Succès ! Le fichier 'donnees_databricks.txt' a été créé.")
        
    else:
        print(f"Erreur lors de la connexion : {response.status_code}")

if __name__ == "__main__":
    scrapper_databricks_rss()