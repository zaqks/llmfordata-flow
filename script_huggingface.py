# 1. IMPORTATION
import requests

def recuperer_huggingface_api():
    # 2. L'ADRESSE DE L'API 
    # On utilise "sort=downloads" pour trier par téléchargements.
    # "direction=-1" veut dire "Descendant" (du plus grand au plus petit).

    url = "https://huggingface.co/api/models?sort=downloads&direction=-1&limit=10"
    
    print("Appel de l'API Hugging Face en cours...")
    
    # 3. ENVOI DE LA DEMANDE
    reponse = requests.get(url)
    
    # 4. VÉRIFICATION
    if reponse.status_code == 200:
        
        # 5. TRANSFORMATION EN PYTHON (JSON)
        liste_modeles = reponse.json()
        
        # 6. CRÉATION DU FICHIER
        with open("donnees_huggingface.txt", "w", encoding="utf-8") as fichier:
            fichier.write("=== TOP MODÈLES HUGGING FACE (PAR TÉLÉCHARGEMENTS) ===\n\n")
            
            # 7. BOUCLE SUR LES DONNÉES
            for modele in liste_modeles:
                
                nom = modele.get('id')                            # Nom du modèle
                telechargements = modele.get('downloads')         # Nombre de téléchargements
                likes = modele.get('likes')                       # Nombre de likes
                
                # On reconstruit le lien web
                lien = f"https://huggingface.co/{nom}"
                
                # 8. ÉCRITURE
                fichier.write(f"Modèle : {nom}\n")
                fichier.write(f"Stats  : {telechargements} téléchargements | {likes} likes\n")
                fichier.write(f"Lien   : {lien}\n")
                fichier.write("-" * 50 + "\n")
                
        print("Succès ! Le fichier 'donnees_huggingface.txt' est prêt.")
        
    else:
        # Si ça échoue encore, on verra le code d'erreur
        print(f"Erreur de l'API : {reponse.status_code}")

if __name__ == "__main__":
    recuperer_huggingface_api()