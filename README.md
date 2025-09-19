UIT Pipeline – Connectivité Numérique
Ce projet contient un pipeline Big Data complet pour l’ingestion, le nettoyage et la valorisation des indicateurs de connectivité mobile et Internet issus de l’UIT.

🔧 Technologies utilisées
Apache NiFi

Apache Kafka

Apache Spark (PySpark)

PostgreSQL

MinIO

Apache Airflow

Apache Superset

Docker Compose

📁 Structure du dépôt
spark-apps/etl_kafka_cleaning.py : script Spark principal pour le nettoyage et la transformation

docker-compose.yml : orchestration des services via Docker

images/ : schémas et visuels du pipeline

.vscode/ : configuration locale

📊 Objectif
Automatiser le traitement des données UIT :

Ingestion via NiFi et Kafka

Nettoyage et typage via PySpark

Diffusion vers :

PostgreSQL (base structurée)

MinIO (uit-cleaned/processed/) au format .parquet

Kafka (uit_connectivite_cleaned) pour réutilisation

📦 Exemple de données nettoyées
Les fichiers .parquet générés contiennent des indicateurs tels que :

Couverture mobile (2G, 3G, 4G, 5G)

Abonnements actifs

Usage d’Internet par pays et par année

👩‍💻 Auteur
Mariéta Sow – Master 2 IA – Data Engineering – DIT