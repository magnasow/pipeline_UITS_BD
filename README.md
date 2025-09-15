# UIT Pipeline – Connectivité Numérique

Ce projet contient un pipeline Big Data complet pour l’ingestion, le nettoyage et la valorisation des indicateurs de connectivité numérique.

## 🔧 Technologies utilisées
- Apache NiFi
- Apache Kafka
- Apache Spark
- PostgreSQL
- MinIO
- Airflow
- Superset
- Docker Compose

## 📁 Structure du dépôt
- `spark-apps/etl_kafka_cleaning.py` : script Spark principal
- `docker-compose.yml` : orchestration des services
- `.vscode/` : configuration locale

## 📊 Objectif
Nettoyer les données brutes issues de Kafka, les structurer, les diffuser vers PostgreSQL, MinIO et Kafka (topic nettoyé).

## 👩‍💻 Auteur
Mariéta Sow – Master 2 IA – Data Engineering – UFR ST, UASZ
