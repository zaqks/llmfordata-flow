import os
from collections import Counter

def analyser_donnees():
    # 1. LISTE DES FICHIERS À ANALYSER
    fichiers = ["donnees_arxiv.txt", "donnees_databricks.txt", "donnees_huggingface.txt"]
    
    # 2. MOTS-CLÉS À SURVEILLER 
    mots_cles = [
        "LLM", "Generative", "ETL", "Data Quality", "AutoML", 
        "RAG", "Transformer", "Pipeline", "Governance", "Lakehouse"
    ]
    
    compteur_global = Counter()
    contenu_total = ""

    print("Démarrage de l'analyse des tendances...\n")

    # 3. LECTURE DES FICHIERS
    for nom_fichier in fichiers:
        if os.path.exists(nom_fichier):
            try:
                with open(nom_fichier, "r", encoding="utf-8") as f:
                    texte = f.read().upper() # On met tout en majuscule pour comparer facilement
                    contenu_total += texte
                    print(f"-> Lecture de {nom_fichier} : OK")
            except Exception as e:
                print(f"Erreur lecture {nom_fichier}: {e}")
        else:
            print(f"Attention : Le fichier {nom_fichier} n'existe pas encore.")

    # 4. COMPTAGE DES MOTS-CLÉS
    # On compte combien de fois chaque mot-clé apparaît dans tout le texte
    for mot in mots_cles:
        occurences = contenu_total.count(mot.upper())
        compteur_global[mot] = occurences

    # 5. GÉNÉRATION DU RAPPORT
    print("\n" + "="*40)
    print("   RAPPORT DE VEILLE : TENDANCES")
    print("="*40)
    
    # On trie du plus fréquent au moins fréquent
    for mot, count in compteur_global.most_common():
        barre = "█" * (count // 2) 
        print(f"{mot:<15} : {count:>3} mentions  {barre}")

    # 6. SAUVEGARDE DU RAPPORT
    with open("rapport_analyse.txt", "w", encoding="utf-8") as f_rapport:
        f_rapport.write("=== RAPPORT DE TENDANCES ===\n\n")
        for mot, count in compteur_global.most_common():
            f_rapport.write(f"{mot} : {count}\n")
            
    print("\nSuccès ! Rapport sauvegardé dans 'rapport_analyse.txt'")

if __name__ == "__main__":
    analyser_donnees()