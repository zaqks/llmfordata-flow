# 1. On importe la bibliothèque 'arxiv' qu'on vient d'installer.
# C'est comme ouvrir la boîte à outils spécifique à ce site.
import arxiv

def recuperer_arxiv():
    print("Début de la récupération arXiv")

    # 2. On prépare notre recherche.
    # query="Generative AI" : C'est le mot-clé qu'on tape dans la barre de recherche.
    # max_results=5 : On demande seulement les 5 derniers articles.

    search = arxiv.Search(
        query="Generative AI",
        max_results=5,
        sort_by=arxiv.SortCriterion.SubmittedDate         # On trie par date de soumission
    )



    # 3. On ouvre (ou crée) un fichier texte pour noter les résultats.
    # "w" signifie "write" (écrire). Si le fichier existe, on l'écrase.
    # encoding="utf-8" permet de gérer les accents correctement.


    with open("donnees_arxiv.txt", "w", encoding="utf-8") as fichier:
        
        fichier.write(" RAPPORT ARXIV \n\n")          # \n veut dire "saut de ligne"

        # 4. La boucle magique "for"
        # search.results() est une liste de papiers.
        # La boucle dit : "Pour chaque 'resultat' trouvé dans la liste..."


        for resultat in search.results():
            
            # On écrit le titre dans le fichier
            fichier.write(f"Titre : {resultat.title}\n")
            
            # On écrit la date
            fichier.write(f"Date : {resultat.published}\n")
            
            # On écrit le lien vers le PDF
            fichier.write(f"Lien PDF : {resultat.pdf_url}\n")
            
            # On fait une ligne de séparation 
            fichier.write("-" * 50 + "\n")
            
            # On affiche aussi un petit message dans le terminal pour dire que ça avance
            print(f"Article récupéré : {resultat.title}")

    print("Terminé ! Le fichier donnees_arxiv.txt est prêt.")

# Cette ligne dit à Python : "Si on lance ce fichier, exécute la fonction recuperer_arxiv"
if __name__ == "__main__":
    recuperer_arxiv()