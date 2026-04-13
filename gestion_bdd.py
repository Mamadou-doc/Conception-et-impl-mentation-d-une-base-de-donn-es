import pandas as pd
import mysql.connector as mysqlcon
import nettoyage_donnees as prep_donnees


def connexion_bdd(nom_host, nom_user, mdp_user):
    """
    Crée une connexion au serveur MySQL.

    Args:
        nom_host (str): Le nom de l'hôte où le serveur MySQL est situé.
        nom_user (str): Le nom de l'utilisateur pour se connecter au serveur.
        mdp_user (str): Le mot de passe de l'utilisateur pour se connecter au serveur.

    Returns:
        MySQLConnection: Une instance de connexion au serveur MySQL.
    Raises:
        mysql.connector.Error: Une erreur survient de la part de MySQL pour
                              des raisons qui peuvent inclure des problèmes
                              de connexion réseau, de mauvaises credentials,
                              ou d'autres problèmes liés à la base de données.

    """
    connexion = None
    try:
        connexion = mysqlcon.connect(
            host=nom_host,
            user=nom_user,
            password=mdp_user
        )
    except mysqlcon.Error as e:
        print("Erreur de création de connexion: {e}")
        # Relancer l'exception pour être géré plus haut dans la chaîne d'appel
        raise
    return connexion

def creer_use_bdd(connexion, nom_bdd):
    """
    Crée une nouvelle base de données MySQL si elle n'existe pas déjà
    et la sélectionne pour être utilisée.

    Args:
        connexion (MySQLConnection): La connexion au serveur MySQL où la BDD sera créée.
        nom_bdd (str): Le nom de la BDD à créer et sélectionner.

    Raises:
        mysql.connector.Error: Une erreur survient liée à MySQL pour des raisons
        qui peuvent inclure des problèmes de permissions, erreurs de syntaxe
        dans le nom de BDD, ou problèmes de connexion réseau.
    """

    try:
        #à compléter
        #-----------------------------------------------------------------------#
        # On crée d'abord un curseur pour pouvoir pour envoyer des requêtes SQL à MySQL.
        curseur=connexion.cursor()
        # ensuite on crée la base si elle n'existe pas avec un f-string 
        curseur.execute(f"CREATE DATABASE IF NOT EXISTS {nom_bdd} ") 
        # On séléctionne la base de données crée 
        curseur.execute(f"USE {nom_bdd}")
        # On valides les changements sinon MySQL ne gardera pas les ce qu'on vient de faire.  
        connexion.commit()
        print(f"Base de données '{nom_bdd}' créée et sélectionnée avec succès.")

    except mysqlcon.Error as e:
        #à compléter
        #-----------------------------------------------------------------------#
        # On capture l'erreur et on l'afficher
        print(f"erreur de type {e} lors de la création de la base de données")
        raise
    finally:
        # On ferme le cuseur car c'est une bonne pratique 
        curseur.close()


def creer_tables(connexion):
    """
    Crée les tables nécessaires pour la base de données si elles n'existent
    pas déjà.

    Args:
        connexion (MySQLConnection): La connexion au serveur MySQL utilisée
        pour exécuter les requêtes.

    Raises:
        mysql.connector.Error: Erreur levée lors de problèmes de connexion à la BDD ou
                               lors de l'exécution des requêtes SQL. Cela peut inclure
                               des erreurs de syntaxe SQL, des violations de contraintes,
                               ou des problèmes de connexion au serveur MySQL.
    """

    try:
        #à compléter
        #-----------------------------------------------------------------------#
        curseur=connexion.cursor()
        # Création de la table CATEGORIE 
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS CATEGORIE (
                id_categorie INT AUTO_INCREMENT PRIMARY KEY,
                nom_categorie VARCHAR(255) NOT NULL UNIQUE 
                 )
            """)
        # Création de la table MARQUE
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS MARQUE (
                id_marque INT AUTO_INCREMENT PRIMARY KEY,
                nom_marque VARCHAR(255) NOT NULL UNIQUE
            )
        """)
        # Création de la table INGREDIENT
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS INGREDIENT (
                id_ingredient INT AUTO_INCREMENT PRIMARY KEY,
                nom_ingredient VARCHAR(255) NOT NULL UNIQUE
            )
        """)
        # Création de la table ADDITIF
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS ADDITIF (
                id_additif INT AUTO_INCREMENT PRIMARY KEY,
                nom_additif VARCHAR(255) NOT NULL UNIQUE
            )
        """)
        # Création de la table ALLERGENE
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS ALLERGENE (
                id_allergene INT AUTO_INCREMENT PRIMARY KEY,
                nom_allergene VARCHAR(255) NOT NULL UNIQUE
            )
        """)
        # Création de la table PRODUIT
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS PRODUIT (
                id_produit INT AUTO_INCREMENT PRIMARY KEY,
                nom VARCHAR(255) NOT NULL UNIQUE,
                nutritionGradeFr VARCHAR(5),
                energie100g FLOAT,
                graisse100g FLOAT,
                sucres100g FLOAT,
                fibres100g FLOAT,
                proteines100g FLOAT,
                sel100g FLOAT,
                presenceHuilePalme TINYINT(1),
                id_categorie INT,
                FOREIGN KEY ( id_categorie) REFERENCES CATEGORIE(id_categorie)
            )
        """)
        # Table d'association PRODUIT_MARQUE
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS PRODUIT_MARQUE (
                id_produit INT, 
                id_marque INT, 
                PRIMARY KEY (id_produit, id_marque),
                FOREIGN KEY (id_produit) REFERENCES PRODUIT(id_produit),
                FOREIGN KEY (id_marque) REFERENCES MARQUE(id_marque)
            ) 
        """)
        # Table d'association PRODUIT_INGREDIENT
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS PRODUIT_INGREDIENT (
                id_produit INT,
                id_ingredient INT,
                PRIMARY KEY (id_produit, id_ingredient),
                FOREIGN KEY (id_produit) REFERENCES PRODUIT(id_produit),
                FOREIGN KEY (id_ingredient)  REFERENCES INGREDIENT(id_ingredient)
                ) 
            """)
        # Table d'association PRODUIT_ADDITIF
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS PRODUIT_ADDITIF (
                id_produit INT,
                id_additif INT,
                PRIMARY KEY (id_produit, id_additif),
                FOREIGN KEY (id_produit) REFERENCES PRODUIT(id_produit),
                FOREIGN KEY (id_additif) REFERENCES ADDITIF(id_additif)
            )
        """)
        # Table d'association PRODUIT_ALLERGENE
        curseur.execute("""
            CREATE TABLE IF NOT EXISTS PRODUIT_ALLERGENE (
                id_produit INT,
                id_allergene INT,
                PRIMARY KEY (id_produit, id_allergene),
                FOREIGN KEY (id_produit) REFERENCES PRODUIT(id_produit),
                FOREIGN KEY (id_allergene) REFERENCES ALLERGENE(id_allergene)
            )
        """)


        connexion.commit()
        print("tables crées avec succés")
    except mysqlcon.Error as e:
        #à compléter
        #-----------------------------------------------------------------------#
        print(f"Erreur lors de la création de la base de données : {e}")
        raise

def inserer_enregistrement(connexion, table, nom_colonne, nom):
    #compléter la docstring
    #---------------------------------------------------------------------------#
    """ 
    Insère un enregistrement dans une table donnée si celui-ci n'existe pas déjà.
    Retourne toujours l'ID de l'enregistrement (existant ou nouvellement créé).

    Paramètres :
        connexion : connexion MySQL active
        table (str) : nom de la table (ex : 'MARQUE')
        nom_colonne (str) : nom de la colonne texte (ex : 'nom_marque')
        nom (str) : valeur à insérer (ex : 'Coca-Cola')

    Retour :
        id_enreg (int) : ID de l'enregistrement dans la table
    """
    id_enreg = None
    curseur = connexion.cursor()
    try:
        # à comlpéter
        #-------------------------------------------------------------------------#
        # on vas déduire automatiquement le nom de la colonne ID
        nom_id=f"id_{table.lower()}"
        # Essayer d'obtenir l'ID existant d'abord et l'enregistrer dans id_enreg
        requete_select=f"SELECT {nom_id} FROM {table} WHERE {nom_colonne}=%s"
        curseur.execute(requete_select, (nom,))
        resultat=curseur.fetchone()
        #sinon il faut insérer l'élément dans la table adéquate et récupérer l'id
        # vous pouvez récupérer l'id avec id_enreg = curseur.lastrowid
        if resultat:
            id_enreg=resultat[0]
        else:
            requete_insert =f"INSERT INTO {table} ({nom_colonne}) VALUE (%s)"
            curseur.execute(requete_insert, (nom,))
            connexion.commit()
            id_enreg=curseur.lastrowid

    except Exception as e:
        # à comlpéter
        #-------------------------------------------------------------------------#
        print(f"Erreur lors de l'insertion dans {table} : {e}")
        raise
    finally:
        curseur.close()
    return id_enreg

def inserer_produit(connexion, nom_prod, nutriscore, presenceHuilePalme, enregies100g, graisse100g, sucres100g,fibres100g, proteines100g, sel100g, id_categorie ):
    """
    Insère un produit dans la table produits avec le nom et le nutriscore fourni.

    Args:
    - connexion (mysql.connector.connection.MySQLConnection): Objet de connexion à la base de données MySQL.
    - nom_prod (str): Nom du produit à insérer.
    - nutriscore (str): Nutriscore du produit ('A', 'B', 'C', 'D', 'E', 'F').
    -presenceHuilePalme(int) Huile de palme(0 ou 1)
    -energie100g, graisse100g, sucres100g, fibres100g,
     proteines100g, sel100g : données nutritionnelles
    -id_categorie (int) : clé étrangère vers la table CATEGORIE 

    Returns:
    - id_prod (int): ID du produit inséré dans la table.

    Raises:
    - Exception: Si une erreur se produit lors de l'insertion du produit.
    """
    id_prod = None
    curseur = connexion.cursor()
    # Convertir les NaN en None pour éviter les erreurs MySQL
    def clean_value(v):
        return None if (v != v) else v  # car NaN != NaN

    enregies100g = clean_value(enregies100g)
    graisse100g = clean_value(graisse100g)
    sucres100g = clean_value(sucres100g)
    fibres100g = clean_value(fibres100g)
    proteines100g = clean_value(proteines100g)
    sel100g = clean_value(sel100g)

    try:
        # à comlpéter
        #-------------------------------------------------------------------------#
        #on vérifie si le produit existe déjà
        requete_select="""
            SELECT id_produit FROM PRODUIT
            WHERE nom =%s
        """
        curseur.execute(requete_select,(nom_prod,))
        resultat=curseur.fetchone()
        # Essayer d'obtenir l'ID existant d'abord et l'enregistrer dans id_prod
        if resultat:
            id_prod=resultat[0]
        #sinon il faut insérer l'élément dans la table adéquate et récupérer l'id
        # vous pouvez récupérer l'id avec id_prod = curseur.lastrowid
        else:
            requete_insert="""
            INSERT INTO PRODUIT (
                nom, nutritionGradeFr, presenceHuilePalme,
                energie100g, graisse100g, sucres100g, fibres100g,
                proteines100g, sel100g, id_categorie
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
        """
            valeurs=(
                nom_prod, nutriscore,presenceHuilePalme,
                enregies100g,graisse100g,sucres100g,fibres100g,
                proteines100g,sel100g,id_categorie
            )
            curseur.execute(requete_insert,valeurs)
            connexion.commit()

            # on récupérer l'ID du nouveau produit
            id_prod=curseur.lastrowid
    

    except Exception as e:
        # à comlpéter
        #-------------------------------------------------------------------------#
        print(f"Erreur lors de l'insertion du produit '{nom_prod}': {e}")
        raise
    finally:
        curseur.close()

    return id_prod


def inserer_associations_prod(connexion, id_prod, marque, categorie, ingredients, allergenes, additifs):
    """
    Insère les associations de produits avec ingrédients, allergènes, additifs, marque et catégorie.

    Parameters:
        connexion (MySQLConnection): Connexion à la base de données.
        id_prod (int): ID du produit.
        ingredients (list): Liste des ingrédients associés au produit.
        allergenes (list): Liste des allergènes associés au produit.
        additifs (list): Liste des additifs associés au produit.
        marque (str): marque de produit
        categorie (str): categorie de produit
    """
    curseur = connexion.cursor()
    try:
        # à compléter
        # ------------------------------------------------------------------------#
        # il faut récupérer l'id de l'enregistrement inséré ou existent et essayer d'insérer
        # dans la table d'association
        # Attention à la duplication de clé (id_enreg, id_prod)
        # --Pour PRODUIT_MARQUE--
        id_marque=inserer_enregistrement(connexion, "MARQUE","nom_marque", marque)

        curseur.execute("""
            INSERT IGNORE INTO PRODUIT_MARQUE(id_produit,id_marque)
            VALUES (%s,%s)
        """,(id_prod, id_marque))
        
        # --Pour PRODUIT_CATEGORIE-- 
        # récupérer id_categorie puis mettre à jour PRODUIT
        id_categorie=inserer_enregistrement(connexion, "CATEGORIE", "nom_categorie", categorie)

        curseur.execute("""
            UPDATE PRODUIT 
            SET id_categorie=%s
            WHERE id_produit=%s
        """, (id_categorie,id_prod))

        #  --Pour PRODUIT_INGREDIENT--
        # On  boucle + association
        for ing in ingredients:
            id_ing=inserer_enregistrement(connexion, "INGREDIENT", "nom_ingredient", ing)

            curseur.execute("""
                INSERT IGNORE INTO PRODUIT_INGREDIENT(id_produit,id_ingredient)
                VALUES (%s,%s)
            """,(id_prod,id_ing))

        #  --Pour PRODUIT_ALLERGENE--
        # meme logique 
        for allg in allergenes:
            id_allg=inserer_enregistrement(connexion, "ALLERGENE", "nom_allergene", allg)

            curseur.execute("""
                INSERT IGNORE INTO PRODUIT_ALLERGENE (id_produit, id_allergene)
                VALUES (%s, %s)
            """,(id_prod, id_allg))

        # --Pour PRODUIT_ADDITIFS-- 
        for add in additifs:
            id_add = inserer_enregistrement(connexion, "ADDITIF", "nom_additif", add)

            curseur.execute("""
                INSERT IGNORE INTO PRODUIT_ADDITIF (id_produit, id_additif)
                VALUES (%s, %s)
            """,(id_prod, id_add))

        connexion.commit()



    except mysqlcon.Error as e:
        # compléter
        # ----------------------------------------------------------------------#
        print(f"Erreur lors de l'insertion des associations du produit {id_prod} : {e}")
        raise

    finally:
        curseur.close()

def verifier_existence_bdd(connexion, nom_bdd):
    # uniquement docstring à compléter
    """
    Vérifie si une base de données MySQL existe sur le serveur.

    Paramètres :
    - connexion : mysql.connector.connection.MySQLConnection
      Connexion active au serveur MySQL (sans sélection préalable de base).
    - nom_bdd (str) :
      Nom de la base de données dont on souhaite vérifier l'existence.

    Retourne :
    - bool :
      True si la base de données existe, False sinon.
      En cas d'erreur MySQL, False est également retourné après affichage d'un message.
    """
    try:
        cursor = connexion.cursor()
        cursor.execute("SHOW DATABASES LIKE %s", (nom_bdd,))
        for database in cursor:
            if database:
                return True
        return False
    except mysqlcon.Error as e:
        print(f"Erreur lors de la vérification de l'existence de la base de données : {e}")
        return False
    finally:
        cursor.close()

def configurer_bdd(connexion, nom_bdd, nom_fichier):

    """
    Configure et peuple la bdd avec des données nettoyées issues d'un fichier CSV.

    Cette fonction crée une base de données et ses tables, nettoie les données
    à partir d'un fichier CSV, et insère les données nettoyées dans la bdd.
    Elle gère les transactions pour assurer que toutes les opérations sont soit
    complètement réalisées, soit annulées en cas d'erreur.

    Paramètres :
    - connexion : mysql.connector.connection.MySQLConnection
      Connexion active à MySQL.
    - nom_fichier : str
      Chemin vers le fichier CSV contenant les données brutes.
    - nom_bdd : str
      Nom de la base de données à créer et utiliser.

    Exceptions :
    - mysql.connector.Error : Gérée si une erreur liée à MySQL survient.
    - FileNotFoundError : Gérée si le fichier CSV n'est pas trouvé.
    - Exception : Gérée pour toute autre erreur inattendue.

    Aucune valeur n'est retournée. Les modifications sont commitées en bdd
    ou annulées si une exception est levée.

    Utilisation :
    >>> configurer_bdd(connexion, 'Data/fr-open-food-facts.csv', 'bdd_aliments')
    """
    try:
        # à compléter: créer la bdd et ses tables, recupérer le jeu de données nettoyé
        creer_use_bdd(connexion,nom_bdd)

        creer_tables(connexion)

        df = prep_donnees.nettoyage_prepation_donnees(nom_fichier)
        # Pour chaque produit, l'insérer dans la bdd avec ses associations
        # Pour cela on parcours de chaque produit
        for index,ligne in df.iterrows():
            # Extraction des données du produit depuis le csv 
            nom_prod = ligne["nom"]
            nutriscore = ligne["nutritionGradeFr"]
            presenceHuilePalme = ligne["presenceHuilePalme"]

            energie = ligne["energie100g"]
            graisses = ligne["graisse100g"]
            sucres = ligne["sucres100g"]
            fibres = ligne["fibres100g"]
            proteines = ligne["proteines100g"]
            sel = ligne["sel100g"]

            marque = ligne["marque"]
            categorie = ligne["categorie"]

            ingredients = ligne["ing_nettoyes"]
            allergenes = ligne["all_nettoyes"]
            additifs = ligne["add_nettoyes"]

            # Insertion du produit ensuite on récupére id_prod
            id_prod=inserer_produit(
                connexion,
                nom_prod,
                nutriscore,
                presenceHuilePalme,
                energie,
                graisses,
                sucres,
                fibres,
                proteines,
                sel,
                None # catégorie sera ajoutée dans les associations

            )
            # Insertion des associations
            inserer_associations_prod(
                connexion,
                id_prod,
                marque,
                categorie,
                ingredients,
                allergenes,
                additifs
            )

        connexion.commit()

        print("Données insérées avec succés")

    except mysqlcon.Error as e:
        #à compléter
        #-----------------------------------------------------------------------#
        print(f"Erreur MySQL lors de la configuration de la BDD : {e}")
        connexion.rollback()
        raise
    except FileNotFoundError:
        print("Erreur : Le fichier spécifié n'a pas été trouvé.")
        # à compléter
        #-----------------------------------------------------------------------#
        raise
    except Exception as e:
        print(f"Erreur inattendue : {e}")
        # à compléter
        #-----------------------------------------------------------------------#
        connexion.rollback()
        raise
