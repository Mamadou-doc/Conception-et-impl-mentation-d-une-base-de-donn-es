import gestion_bdd as bdd
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import mysql.connector as mysqlcon

def tracer_diag_barres(df, x_col, y_col, titre, xlabel, ylabel):
    """
    Trace un diagramme à barres des valeurs spécifiées dans un DataFrame.

    Parameters:
    - df (pd.DataFrame): Le DataFrame contenant les données à visualiser.
    - x_col (str): Nom de la colonne dans df qui représente les valeurs sur l'axe des abscisses.
    - y_col (str): Nom de la colonne dans df qui représente les valeurs sur l'axe des ordonnées.
    - titre (str): Titre à afficher au-dessus du diagramme à barres.
    - xlabel (str): Étiquette de l'axe des abscisses.
    - ylabel (str): Étiquette de l'axe des ordonnées.

    Utilise la bibliothèque matplotlib pour la visualisation et seaborn pour le style.
    La fonction configure un style sombre pour le graphique, ajuste la taille de la figure
    pour une meilleure visibilité et utilise une couleur spécifique pour les barres.
    Les labels des axes et le titre sont mis en valeur par une mise en forme grasse de taille
    16 et 14. Les étiquettes de l'axe des abscisses sont orientées à 90 degrés pour améliorer
    la lisibilité.

    Returns:
    None
    """
    #àcompléter
    #---------------------------------------------------------------------------#
    plt.figure(figsize=(12, 6))
    sns.set_style("darkgrid")

    sns.barplot(data=df, x=x_col, y=y_col, color="steelblue")

    plt.title(titre, fontsize=16, weight="bold")
    plt.xlabel(xlabel, fontsize=14)
    plt.ylabel(ylabel, fontsize=14)

    plt.xticks(rotation=90, fontsize=12)
    plt.yticks(fontsize=12)

    plt.tight_layout()
    plt.show()
    plt.close()

def tracer_diag_circulaire(df, frequences, categories,titre):
    """
    Trace un diagramme circulaire (pie chart) des fréquences pour les catégories spécifiées dans un DataFrame.

    Parameters:
    - df (pd.DataFrame): DataFrame contenant les données à visualiser.
    - frequences (str): Nom de la colonne dans df qui contient les fréquences de chaque catégorie.
    - categories (str): Nom de la colonne dans df qui contient les noms des catégories.
    - titre (str): Titre à afficher au-dessus du diagramme circulaire.

    Cette fonction utilise la bibliothèque matplotlib pour créer le graphique et seaborn pour
    définir le style de visualisation.
    Elle affiche le pourcentage de chaque catégorie sur le diagramme avec des étiquettes
    de grande taille, en gras, et une rotation de 30 degrés.
    Une légende est également incluse pour aider à l'identification des différentes catégories du diagramme.

    Returns:
    None
    """

    # à compléter
    # --------------------------------------------------------------------------#
    plt.figure(figsize=(10, 10))
    sns.set_style("whitegrid")

    valeurs = df[frequences]
    labels = df[categories]

    plt.pie(
        valeurs,
        labels=labels,
        autopct="%1.1f%%",
        startangle=90,
        textprops={"fontsize": 14, "weight": "bold"}
    )

    plt.title(titre, fontsize=16, weight="bold")
    plt.axis("equal")

    plt.legend(labels, loc="best", fontsize=12)
    plt.tight_layout()
    plt.show()
    plt.close()


def tracer_diag_aires_empilees(df, xcol, cols, titre, ylabel):
    """
    Trace un diagramme à aires empilées pour les colonnes spécifiées d'un DataFrame.

    Args:
    df (DataFrame): Le DataFrame contenant les données.
    xcol (str): Le nom de la colonne à utiliser pour l'axe des x.
    cols (list): Liste des noms des colonnes à tracer comme aires empilées.
    titre (str): Le titre du graphique.
    ylabel (str): Le texte pour l'étiquette de l'axe des y.

    Returns:
    None: La fonction génère un graphique mais ne retourne rien.
    """
    plt.figure(figsize=(12, 6))
    sns.set_style("white")

    # Préparer les données pour le stackplot
    data_to_plot = [df[col] for col in cols]

    # Utiliser des couleurs de la palette "colorblind" pour une meilleure accessibilité
    colors = sns.color_palette("colorblind", len(cols))

    # Tracer le diagramme à aires empilées
    plt.stackplot(df[xcol], *data_to_plot, colors=colors, labels=cols, edgecolor='none')
    plt.xticks(rotation=45,fontsize=14, weight="bold" )
    plt.ylabel(ylabel, fontsize=14)
    plt.title(titre, fontsize=16, weight="bold")
    plt.legend(loc="upper left")
    sns.despine()
    plt.show()
    plt.close()



#Création de la BDD et ses tables
if __name__ == "__main__":
    connexion = None
    curseur = None
    try:
        nom_bdd = "bdd_aliments"
        nom_fichier = "fr-open-food-facts.csv"

        #à compléter
        #-----------------------------------------------------------------------#
        #Créer une instance de connexion
        connexion = bdd.connexion_bdd("localhost", "root","mamadou")
        if connexion.is_connected():
            print("Connexion réussie")

        # Tester si la BDR existe déja pour ne pas la recréer et la peupler à chaque
        # lancement de votre script
        # Si la BDR n'existe pas il faut la créer et la remplir
        if not bdd.verifier_existence_bdd(connexion, nom_bdd):
            print("Base inexistante,  créons  et inserons des données...")
            bdd.configurer_bdd(connexion, nom_bdd, nom_fichier)
        else:
            print("Base existante utilisons le directe.")
            bdd.creer_use_bdd(connexion, nom_bdd)



        #Exploration et visualisation
        curseur = connexion.cursor()
        #Insérer ici les requetes d'exploration de la BDR
        # et visualiser avec les fonctions de dessin ci dessus

        # Aide
        # pour transformer les résultats d'une requete en un DataFrame
        # il suffit de créer un DataFrame à partir des résultats en donnant
        # des noms aux colonnes
        # Par exemple:
        # res = curseur.fetchall()
        # df = pd.DataFrame(res, columns=liste des noms colonnes de res)

        # a) Graphique Répartition des produits par Nutri-Score (camembert)
        curseur.execute("""
            SELECT nutritionGradeFr, COUNT(*) 
            FROM PRODUIT
            GROUP BY nutritionGradeFr
            ORDER BY nutritionGradeFr
        """)
        res = curseur.fetchall()
        df = pd.DataFrame(res, columns=["nutriscore", "nb"])
        df = df.dropna()  # enlever les nutriscore NULL
        tracer_diag_circulaire(df, "nb", "nutriscore", "Répartition des produits par Nutri-Score")


        # b) Graphique Présence d’huile de palme (camembert)
        curseur.execute("""
            SELECT presenceHuilePalme, COUNT(*)
            FROM PRODUIT
            GROUP BY presenceHuilePalme
        """)
        res = curseur.fetchall()
        df = pd.DataFrame(res, columns=["presence", "nb"])
        df = df.dropna()
        df["presence"] = df["presence"].map({0: "Absente", 1: "Présente"})
        tracer_diag_circulaire(df, "nb", "presence", "Répartition des produits par Présence/Absence d'huile de palme")


        # c) Graphique Moyennes nutritionnelles par Nutri-Score (aires empilées)

        curseur.execute("""
            SELECT nutritionGradeFr,
                   AVG(graisse100g),
                   AVG(sucres100g),
                   AVG(fibres100g),
                   AVG(proteines100g)
            FROM PRODUIT
            GROUP BY nutritionGradeFr
            ORDER BY nutritionGradeFr
        """)
        res = curseur.fetchall()
        df = pd.DataFrame(res, columns=["nutriscore", "graisse", "sucres", "fibres", "proteines"])
        df = df.dropna()
        tracer_diag_aires_empilees(
            df,
            "nutriscore",
            ["graisse", "sucres", "fibres", "proteines"],
            "Analyse nutritionnelle par Nutri-Score",
            "Moyennes nutritionnelles"
        )

        # d) GraphiqueTop 15 produits les plus complexes (allergènes + additifs)
        curseur.execute("""
            SELECT P.nom,
                   COUNT(DISTINCT PA.id_additif) + COUNT(DISTINCT PAL.id_allergene) AS total_complexite
            FROM PRODUIT P
            LEFT JOIN PRODUIT_ADDITIF PA ON P.id_produit = PA.id_produit
            LEFT JOIN PRODUIT_ALLERGENE PAL ON P.id_produit = PAL.id_produit
            GROUP BY P.id_produit
            ORDER BY total_complexite DESC
            LIMIT 15
        """)
        res = curseur.fetchall()
        df = pd.DataFrame(res, columns=["produit", "complexite"])
        plt.figure(figsize=(12, 8))
        df = df.sort_values(by="complexite", ascending=True)

        plt.barh(df["produit"], df["complexite"], color="skyblue")
        plt.xlabel("Nb allergènes + additifs", fontsize=14)
        plt.ylabel("Produits", fontsize=14)
        plt.title("Top 15 produits les plus complexes", fontsize=16, weight="bold")

        plt.xticks(fontsize=12)
        plt.yticks(fontsize=10)

        plt.tight_layout()
        plt.show()
        plt.close()

        # e) Graphique Top 15 ingrédients les plus utilisés
        curseur.execute("""
            SELECT I.nom_ingredient, COUNT(*) AS nb
            FROM INGREDIENT I
            JOIN PRODUIT_INGREDIENT PI ON I.id_ingredient = PI.id_ingredient
            GROUP BY I.id_ingredient
            ORDER BY nb DESC
            LIMIT 15
        """)
        res = curseur.fetchall()
        df = pd.DataFrame(res, columns=["ingredient", "nb"])
        
        plt.figure(figsize=(14, 6))
        df = df.sort_values(by="nb", ascending=False)

        plt.bar(df["ingredient"], df["nb"], color="navy")
        plt.title("Top 15 ingrédients les plus utilisés", fontsize=16, weight="bold")
        plt.xlabel("Ingrédients", fontsize=14)
        plt.ylabel("Nombre d'apparitions", fontsize=14)

        plt.xticks(rotation=45, ha="right", fontsize=10)
        plt.tight_layout()
        plt.show()
        plt.close()


    except mysqlcon.Error as e:
        print(f"Erreur lors de la connexion à la base de données MySQL: {e}")
    except ValueError as e:
        print(f"Erreur : Problème avec les données: {e}")
    except TypeError as e:
        print(f"Erreur : Problème avec le type des paramètres: {e}")
    except Exception as e:
        print(f"Erreur inattendue : {e}")
    finally:
        #fermeture de curseur
        if curseur:
            curseur.close()
        #Fermeture de la connexion
        if connexion:
            connexion.close()
