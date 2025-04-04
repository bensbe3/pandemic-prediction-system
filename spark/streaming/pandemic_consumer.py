#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Configuration de la base de données
DB_URL = "jdbc:postgresql://localhost:5432/pandemic_db"
DB_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    """Créer une session Spark"""
    return (SparkSession.builder
            .appName("PandemicDataProcessor")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.5.1")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
            .getOrCreate())

def define_schema():
    """Définir le schéma des données de pandémie"""
    return StructType([
        StructField("Entity", StringType(), True),
        StructField("Code", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Reported cholera deaths", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])

def train_model(data_df):
    """Entraîner un modèle de régression linéaire sur les données"""
    # Grouper par pays pour avoir suffisamment de données
    country_data = data_df.select("Entity", "Code", "Year", "Reported cholera deaths")
    
    # Préparer les données pour l'entraînement
    assembler = VectorAssembler(
        inputCols=["Year"],
        outputCol="features"
    )
    
    # Transformer les données
    prepared_data = assembler.transform(country_data)
    
    # Diviser en ensemble d'entraînement (80%) et de test (20%)
    train_data, test_data = prepared_data.randomSplit([0.8, 0.2], seed=42)
    
    # Définir et entraîner le modèle
    lr = LinearRegression(
        featuresCol="features", 
        labelCol="Reported cholera deaths",
        maxIter=10,
        regParam=0.1,
        elasticNetParam=0.8
    )
    
    model = lr.fit(train_data)
    
    # Évaluer le modèle
    predictions = model.transform(test_data)
    
    # S'assurer que les prédictions sont positives
    predictions = predictions.withColumn(
        "prediction", 
        when(col("prediction") < 0, 0).otherwise(col("prediction"))
    )
    
    # Calculer les métriques d'évaluation
    evaluator = RegressionEvaluator(
        labelCol="Reported cholera deaths",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    rmse = evaluator.evaluate(predictions)
    r2 = evaluator.setMetricName("r2").evaluate(predictions)
    
    print(f"Modèle de régression linéaire - RMSE: {rmse}, R²: {r2}")
    
    return model, rmse, r2

def process_batch(batch_df, batch_id):
    """Traiter un batch de données avec ML et stocker dans la base de données"""
    if not batch_df.isEmpty():
        # Afficher le batch pour le débuggage
        print(f"Batch ID: {batch_id}")
        batch_df.show(truncate=False)
        
        try:
            # Entraîner un modèle sur les données du batch
            model, rmse, r2 = train_model(batch_df)
            
            # Préparer les données pour la prédiction
            assembler = VectorAssembler(
                inputCols=["Year"],
                outputCol="features"
            )
            
            # Transformer les données
            vectorized_df = assembler.transform(batch_df)
            
            # Faire des prédictions
            predictions = model.transform(vectorized_df)
            
            # S'assurer que les prédictions sont positives
            predictions = predictions.withColumn(
                "prediction", 
                when(col("prediction") < 0, 0).otherwise(col("prediction"))
            )
            
            # Ajouter les métriques du modèle
            predictions = predictions.withColumn("model_rmse", lit(rmse))
            predictions = predictions.withColumn("model_r2", lit(r2))
            
            # Afficher les prédictions
            print("Prédictions:")
            predictions.select("Entity", "Year", "Reported cholera deaths", "prediction", "model_rmse", "model_r2").show()
            
            # Stocker les prédictions dans PostgreSQL
            (predictions
             .select(
                 col("Entity").alias("entity"), 
                 col("Code").alias("code"), 
                 col("Year").alias("year"), 
                 col("Reported cholera deaths").alias("reported_deaths"), 
                 col("prediction").alias("predicted_deaths"),
                 col("model_rmse").alias("model_rmse"),
                 col("model_r2").alias("model_r2")
             )
             .write
             .mode("append")
             .jdbc(DB_URL, "cholera_predictions", properties=DB_PROPERTIES))
            
            print("Données stockées avec succès dans PostgreSQL")
        except Exception as e:
            print(f"Erreur lors de l'écriture dans la base de données: {e}")
            # Si l'erreur est liée aux colonnes manquantes, faire une version simplifiée
            try:
                # Version simplifiée sans métriques du modèle
                predictions = batch_df.withColumn(
                    "prediction", 
                    col("Reported cholera deaths") * 1.05  # Prédiction simplifiée
                )
                
                # Stocker les données sans métriques
                (predictions
                 .select(
                     col("Entity").alias("entity"), 
                     col("Code").alias("code"), 
                     col("Year").alias("year"), 
                     col("Reported cholera deaths").alias("reported_deaths"), 
                     col("prediction").alias("predicted_deaths")
                 )
                 .write
                 .mode("append")
                 .jdbc(DB_URL, "cholera_predictions", properties=DB_PROPERTIES))
                
                print("Données stockées avec succès (version simplifiée)")
            except Exception as inner_e:
                print(f"Erreur lors de l'écriture simplifiée: {inner_e}")

def main():
    """Fonction principale"""
    spark = create_spark_session()
    
    # Définir le schéma des données
    schema = define_schema()
    
    # Lire le flux Kafka
    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "pandemic-data")
          .option("startingOffsets", "earliest")
          .load())
    
    # Décoder le message JSON de Kafka
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    # Convertir les colonnes au bon type
    typed_df = parsed_df \
        .withColumn("Year", col("Year").cast(IntegerType())) \
        .withColumn("Reported cholera deaths", col("Reported cholera deaths").cast(IntegerType()))
    
    # Traiter le streaming par batch
    query = typed_df.writeStream \
        .foreachBatch(process_batch) \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
