# Conception-et-impl-mentation-d-une-base-de-donn-es Open Food Facts 🍎 

## 📌 Description

Ce projet a pour objectif de concevoir et implémenter une base de données relationnelle à partir du dataset Open Food Facts.

Il couvre l’ensemble du pipeline de traitement de données :
- Nettoyage et préparation des données
- Modélisation de la base de données (MCD, MLD, MPD)
- Création et alimentation d’une base MySQL
- Analyse et visualisation des données

---

## ⚙️ Technologies utilisées

- Python
- Pandas
- MySQL
- Matplotlib / Seaborn

---

## 🧱 Architecture du projet

- `nettoyage_donnees.py` : nettoyage et préparation des données  
- `gestion_bdd.py` : création et gestion de la base MySQL  
- `principal.py` : orchestration + visualisation
- `fr-open-food-facts.csv` # Dataset 


---

## 🔄 Pipeline de traitement

1. Chargement du dataset Open Food Facts
2. Nettoyage des données :
   - gestion des valeurs manquantes
   - suppression des doublons
   - normalisation des variables
   - extraction des listes (ingrédients, allergènes, additifs)
3. Modélisation relationnelle :
   - tables PRODUIT, MARQUE, CATEGORIE, INGREDIENT, etc.
   - tables d’association (relations N-N)
4. Insertion en base MySQL avec gestion des doublons
5. Analyse exploratoire et visualisation

---

## 🧠 Points techniques clés

- Gestion des relations plusieurs-à-plusieurs avec tables d’association
- Utilisation de requêtes SQL dynamiques
- Nettoyage avancé de chaînes avec regex
- Pipeline automatisé d’ingestion de données
- Gestion des NaN pour compatibilité MySQL
- Prévention des doublons via requêtes SELECT + INSERT

---

## 📊 Analyses réalisées

- Répartition des produits par Nutri-Score
- Présence d’huile de palme
- Analyse nutritionnelle par catégorie
- Produits les plus complexes (additifs + allergènes)
- Ingrédients les plus utilisés

---

## 🚀 Lancer le projet

1. Installer les dépendances :
```bash
pip install pandas matplotlib seaborn mysql-connector-python
python principal.py
