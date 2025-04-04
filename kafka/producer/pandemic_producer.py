#!/usr/bin/env python3

import os
import json
import time
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime
import subprocess

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'pandemic-data'
HDFS_DIR = f'/data/pandemics/{datetime.now().strftime("%Y-%m-%d")}'
TEMP_DIR = '/tmp/pandemic-data'

def create_kafka_producer():
    """Créer un producteur Kafka"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def read_data_from_hdfs():
    """Lire les données depuis HDFS et les stocker temporairement"""
    # Créer un répertoire temporaire
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    # Lister les fichiers dans HDFS
    cmd = f"hdfs dfs -ls {HDFS_DIR}"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, text=True)
    files = [line.split()[-1] for line in result.stdout.split('\n') if '.csv' in line]
    
    # Télécharger chaque fichier
    for hdfs_file in files:
        local_file = os.path.join(TEMP_DIR, os.path.basename(hdfs_file))
        cmd = f"hdfs dfs -get {hdfs_file} {local_file}"
        subprocess.run(cmd, shell=True)
        
        # Lire et traiter le fichier
        df = pd.read_csv(local_file)
        for _, row in df.iterrows():
            yield row.to_dict()

def main():
    """Fonction principale"""
    producer = create_kafka_producer()
    
    print("Démarrage du producteur Kafka pour les données de pandémie...")
    
    for data in read_data_from_hdfs():
        # Ajouter un timestamp pour le streaming
        data['timestamp'] = datetime.now().isoformat()
        
        # Envoyer les données à Kafka
        producer.send(KAFKA_TOPIC, value=data)
        producer.flush()
        
        print(f"Données envoyées: {data}")
        
        # Simuler le streaming en temps réel avec un délai
        time.sleep(1)

if __name__ == "__main__":
    main()
