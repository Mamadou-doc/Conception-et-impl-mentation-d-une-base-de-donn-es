import pandas as pd
# Le module re pour les expressions régulières, 
# effectuer des opérations complexes de recherche et de manipulation de chaînes
import re


def get_cols_donnees_nutri(df):
    """
    Retourne une liste des noms de colonnes de données nutritionnelles à partir d'un DataFrame.

    Args:
    - df (pandas.DataFrame): Le DataFrame contenant les données nutritionnelles.

    Returns:
    - cols_donnee_nutri (list): Une liste des noms de colonnes de données nutritionnelles,
                                excluant les colonnes 'categorie', 'marque', 'nom',
                                'nutritionGradeFr', 'ingredients', 'allergenes' et 'additifs', 'ing_nettoyes', 'add_nettoyes', 'all_nettoyes', 'presenceHuilePalme'
    """
    #noms de colonnes de données non nutritionnelles
    cols_non_nutri=['categorie', 'marque', 'nom', 'nutritionGradeFr', 'ingredients','allergenes','additifs','ing_nettoyes', 'add_nettoyes', 'all_nettoyes', 'presenceHuilePalme']

    cols_donnee_nutri = []
    for col in  df.columns.tolist():
        if col not in cols_non_nutri:
            cols_donnee_nutri.append(col)
    return cols_donnee_nutri


def convertir_en_liste(chaine):
    """
    Convertit une chaîne de caractères en une liste de chaine en utilisant
    la virgule comme séparateur.

    Parameters:
        chaine (str): La chaîne à convertir.

    Returns:
        list: Une liste de chaines nettoyées.
    """
    #strip(): retirer les espaces blancs au début et à la fin d'une chaîne
    return [item.strip() for item in chaine.split(',') if item.strip() != '']

def supprimer_doublons(liste):
    """
    Élimine les doublons d'une liste en utilisant un ensemble (set).

    Parameters:
        liste (list): La liste initiale contenant des doublons.

    Returns:
        list: Une liste sans doublons.
    """
    #fonction set() renvoie une collection où chaque élément est unique
    # et immuable
    return list(set(liste))


def capitaliser_elements_lst(liste):
    """
    Transforme chaque chaîne de caractères dans une liste en mettant en majuscule la première lettre de chaque élément et en minuscules les autres lettres.

    Parameters:
        liste (list of str): Liste des chaînes de caractères à transformer.

    Returns:
        list of str: Liste des chaînes avec la première lettre de chaque élément en majuscule.
    """
    return [item.capitalize() for item in liste]

def nettoyer_et_extraire_ingredients(chaine):
    """
    Nettoie une chaîne de texte contenant des ingrédients en supprimant les caractères indésirables,
    en séparant les ingrédients listés avec divers séparateurs, et en normalisant l'orthographe.

    Parameters:
        chaine (str): Chaîne de caractères contenant la liste brute des ingrédients.

    Returns:
        list of str: Liste des ingrédients nettoyés et formatés.
    """
    # Supprimer les underscores et autres caractères spéciaux
    '''
    re.sub(pattern, repl, string): Cette fonction du module re remplace les occurrences du motif pattern dans la chaîne string par repl. Ici, r'[_*]' représente un motif qui cherche tous les caractères underscore _ et astérisque *. Ces caractères sont remplacés par une chaîne vide '', c'est-à-dire qu'ils sont supprimés du texte.
    '''
    chaine = re.sub(r'[_*]', '', chaine)

    # Gérer les multiples séparateurs (virgules, points-virgules, et autres)
    #après ; espace ou , espace ou - entouré d'espaces ou . espace et une lettre
    #ou * on divise la chaine.
    separateurs = r'; |, | - |\. (?=[a-zA-Z])|\*'
    ingredients = re.split(separateurs, chaine)

    # Nettoyer les espaces superflus, retirer les points restants, et capitaliser chaque ingrédient
    ingredients = [ing.strip('.').strip().lower().capitalize() for ing in ingredients if ing.strip() != '']

    return ingredients


def nettoyage_prepation_donnees(nom_fichier):
    """
    Cette fonction charge un fichier CSV, effectue un nettoyage de données complet, et prépare les données pour une analyse ultérieure.

    Étapes de traitement :
    1. Conversion des types de données où nécessaire, par exemple convertir la colonne huile de palme en booléen.
    2. Gestion des données manquantes : affiche les informations de données manquantes, supprime les colonnes avec plus de 70% de valeurs manquantes, et impute certaines colonnes avec des valeurs spécifiées.
    3. Détection et suppression des duplications dans le DataFrame.
    4. Normalisation des données pour corriger les incohérences et les fautes de frappe, par exemple en mettant le nutriscore en majuscules.
    5. Vérification et traitement des valeurs aberrantes.
    6. Préparation des données pour la conception de Modèle Conceptuel de Données (MCD) et Modèle Logique de Données (MLD), y compris la gestion des cardinalités entre produits, marques et catégories.

    Paramètres :
    - nom_fich (str): Le chemin vers le fichier CSV à charger.

    Retourne :
    - pd.DataFrame : Le DataFrame nettoyé et préparé.
    """

    donnees = pd.read_csv(nom_fichier,sep = '|')
    print(donnees.head(5))

    ###########################
    #étape 1: Type de donnees #
    print(donnees.dtypes) # On voit que les données numériques sont en float et les chaines de caractères de types object: pas besoin d'une conversion de type

    ##############################
    #étape 2: Données manquantes #
    print(donnees.isna)
    ##############################
    #---------------------------------------------------------------------------#
    # 1. Afficher le total de NaN par colonne
    print("Nombre totale de valeur maquante par colonne")
    print(donnees.isna().sum())
    # 2. Afficher le pourcentage de NaN par colonne
    print("Pourcentage de valeur manquante par colonnes")
    pourcentage_nan= (donnees.isna().sum()/len(donnees))*100
    print(pourcentage_nan)
    # 3. Supprimer les colonnes dont le pourcentage est suppérieur à 70% 
    cols_a_supprimer = pourcentage_nan[pourcentage_nan>=70].index.to_list() # nous on as besoin des colonne et pas des valeurs, donc on affiche les indexes. 
    print(f"Voici les colonnes à supprimer {cols_a_supprimer}")
    donnees=donnees.drop(columns=cols_a_supprimer)
    # 4. Remplacer NaN dans allergènes et additifs
    if "allergenes" in donnees.columns:
        donnees["allergenes"]=donnees["allergenes"].fillna("Non spécifié")
    if "additifs" in donnees.columns:
        donnees["additifs"]=donnees["additifs"].fillna("Non spécifié")


    #étape 3: Trouver les duplications
    ###############################
    # à compléter
    #---------------------------------------------------------------------------#
    print("nombre de ligne avant suppression de données duppliquer")
    print(len(donnees))
    print("Nombre de ligne dupliqués : ")
    print(donnees.duplicated().sum())
    donnees= donnees.drop_duplicates()
    print("nombre de ligne après suppression")
    print(len(donnees))

    ##############################
    #étape 4: Incohérences et Typos
    ###############################
    # à compléter
    #---------------------------------------------------------------------------#
    # Verrifion d'abord si la colonne est belle est bien dans les données 
    if "nutritionGradeFr" in donnees.columns:
        donnees["nutritionGradeFr"]= donnees["nutritionGradeFr"].str.upper()
    valeur_valides=["A", "B", "C", "D", "E"]
    donnees.loc[~donnees["nutritionGradeFr"].isin(valeur_valides), "nutritionGradeFr"]=None
    print(" Nutri-score uniformisée")

    # nettoyage des ingrédients
    if "ingredients" in donnees.columns:
    # Appliquer la fonction de nettoyage sur chaque ligne
        donnees["ing_nettoyes"]=donnees["ingredients"].apply(
             lambda x: nettoyer_et_extraire_ingredients(x) if isinstance(x, str) else []
        )
        print("les ingredients sont extrait et propres dans la colonne 'ing_nettoyes' crée")
    # Nettoyage des allergènes 
    if "allergenes" in donnees.columns:
        donnees["all_nettoyes"]=donnees["allergenes"].apply(
            lambda val : []
            if val =="Non spécifié" or not isinstance(val, str)
            else list({
                item.lower().capitalize()
                for item in convertir_en_liste(val)
            })
        )
        print("Les allergènes sont nettoyés dans la colonne 'all_nettoyes'.")
        # Nettoyage des additifs et création de la nouvelle colonne
    if "additifs" in donnees.columns:
        donnees["add_nettoyes"]=donnees["additifs"].apply(
            lambda val : []
            if val=="Non spécifié" or not isinstance(val, str)
            else list({
                item.lower().capitalize()
                for item in convertir_en_liste(val)
            })
        )
        

    ##############################
    #étape 5: les valeurs aberrantes
    ###############################
    # à compléter
    # ----------------------------------------------------------------------------#
    # Récupérer les colonnes nutritionnelles
    cols_nutri = get_cols_donnees_nutri(donnees)

    print("Résumé statistique des colonnes nutritionnelles :")
    print(donnees[cols_nutri].describe())

    return donnees


if __name__ == "__main__":
    df=nettoyage_prepation_donnees("fr-open-food-facts.csv")
    print("Nombre final de lignes après nettoyage :", len(df))
    print("Nombre de noms de produits uniques :", df["nom"].nunique())
