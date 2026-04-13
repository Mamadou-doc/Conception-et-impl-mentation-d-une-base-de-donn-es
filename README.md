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

---

## 🔄 Pipeline

1. Chargement du dataset
2. Nettoyage des données
3. Modélisation relationnelle
4. Insertion en base MySQL
5. Analyse et visualisation

---

## 🧠 Points techniques

- Relations plusieurs-à-plusieurs (tables d’association)
- Requêtes SQL dynamiques
- Nettoyage avec regex
- Gestion des NaN pour MySQL
- Pipeline automatisé

---

## 🚀 Lancer le projet

```bash
pip install pandas matplotlib seaborn mysql-connector-python
python principal.py
