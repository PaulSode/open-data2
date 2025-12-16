# TP2 - Pipeline Open Data

## Description
Le pipeline utilise les APIs suivantes :

- **OpenFoodFacts** (produits alimentaires) : source principale
- **API Adresse (Base Adresse Nationale)** : géocodage des points de vente pour enrichissement

Les données finales sont stockées en Parquet, avec génération automatique d’un rapport de qualité.

---

## Objectifs pédagogiques

À l’issue du TP, le pipeline permet de :

1. Interroger plusieurs APIs REST
2. Enrichir les données en croisant plusieurs sources
3. Gérer pagination, erreurs et rate limiting
4. Calculer un scoring de qualité automatique
5. Nettoyer et transformer les données avec des règles métier
6. Stocker les données au format Parquet
7. Générer un rapport Markdown complet
8. Tester et valider la couverture du code

---

## Structure du projet

```bash
open-data2_bis/
├── .env
├── pyproject.toml
├── README.md
├── pipeline/
│   ├── init.py
│   ├── config.py
│   ├── models.py
│   ├── fetchers/
│   │ ├── init.py
│   │ ├── base.py
│   │ ├── openfoodfacts.py
│   │ └── adresse.py
│   ├── enricher.py
│   ├── transformer.py
│   ├── quality.py
│   ├── storage.py
│   └── main.py
├── tests/
│   ├── init.py
│   ├── test_fetchers.py
│   ├── test_transformer.py
│   └── test_others.py
├── data/
│   ├── raw/
│   ├── processed/
│   └── reports/
└── notebooks/
    └── exploration.ipynb
```

---

## Installation
```bash
uv add httpx pandas duckdb litellm python-dotenv tenacity tqdm pyarrow pydantic pytest
```

## Configuration

Les principaux paramètres sont centralisés dans pipeline/config.py :
 - OPENFOODFACTS_CONFIG et ADRESSE_CONFIG : paramètres API
 - MAX_ITEMS, BATCH_SIZE : limites d’acquisition
 - QUALITY_THRESHOLDS : seuils pour la qualité
 - RAW_DIR, PROCESSED_DIR, REPORTS_DIR : dossiers de stockage

## Utilisation

### Lancer le pipeline

Test pour quelques valeurs (car type chocolats), ignore la partie Enrichissement

```bash
python -m pipeline.main --category chocolats --max-items 100 --verbose
```

Test pour de nombreuses valeurs, inclue la partie Enrichissement


```bash
python -m pipeline.main --category "orange juice" --max-items 500 --verbose
```

Options :
--category, -c : catégorie de produits OpenFoodFacts
--max-items, -m : nombre maximum de produits à récupérer
--skip-enrichment, -s : ignorer l’étape de géocodage
--verbose, -v : afficher la progression

### Résultats attendus
Données brutes JSON : data/raw/
Données transformées Parquet : data/processed/
Rapport de qualité Markdown : data/reports/


## Structure du pipeline

**Acquisition : fetchers/**
Récupération des données avec gestion du rate limiting, pagination et retry automatique.

**Enrichissement : enricher.py**
Extraction des adresses depuis stores, géocodage via API Adresse, enrichissement des produits avec coordonnées.

**Transformation : transformer.py**
Suppression des doublons
Gestion des valeurs manquantes
Normalisation texte
Ajout de colonnes dérivées (sugar_category, is_geocoded)

**Qualité : quality.py**
Calcul des métriques : complétude, doublons, géocodage, valeurs nulles.
Génération d’un rapport Markdown avec recommandations IA simulées.

**Stockage : storage.py**
Sauvegarde JSON pour les données brutes et Parquet pour les données traitées.


## Tests
### Exécution
```bash
pytest tests/ -v --cov=pipeline --cov-report=html
```

Génère un rapport HTML dans htmlcov/index.html

 - Tests unitaires pour les fetchers (test_fetchers.py)
 - Tests pour le transformer (test_transformer.py)
 - Tests pour enricher, quality et storage (test_others.py)


## Technologies et dépendances principales

 - Python 3.10+
 - httpx pour les requêtes API
 - pandas pour le traitement de données
 - pydantic v2 pour les modèles et validation
 - pytest et pytest-cov pour les tests et couverture
 - tqdm pour la barre de progression
 - litellm pour la génération de recommandations IA (simulée dans les tests)

 ### Par Paul Sode