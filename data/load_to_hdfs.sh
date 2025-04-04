#!/bin/bash

# Créer le répertoire dans HDFS s'il n'existe pas
hdfs dfs -mkdir -p /data/pandemics/$(date +%Y-%m-%d)

# Copier les données du bureau vers HDFS
hdfs dfs -put -f /home/vboxuser/Desktop/deaths.csv /data/pandemics/$(date +%Y-%m-%d)/

echo "Les données ont été chargées dans HDFS avec succès."
