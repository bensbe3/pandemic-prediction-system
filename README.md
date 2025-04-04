# Pandemic Prediction System

A Big Data architecture for predicting pandemic spread patterns using historical cholera data.

## Architecture Overview

This system integrates five major Big Data technologies to create an end-to-end pipeline for processing epidemiological data:

- **Data Collection**: Raw pandemic data stored in HDFS
- **Kafka**: Real-time data streaming pipeline
- **Spark Streaming**: Processing and machine learning predictions
- **PostgreSQL**: Persistent storage of predictions and results
- **Streamlit**: Interactive data visualization dashboard

## Technologies Used

- **Apache Hadoop (HDFS)**
- **Apache Kafka**
- **Apache Spark**
- **PostgreSQL**
- **Streamlit**

## Installation & Setup

### Prerequisites

Before setting up the system, ensure you have the following installed:

- Ubuntu Linux (18.04 or later)
- Java 8 or later
- Python 3.8 or later
- Git

### Step 1: Clone the Repository

git clone https://github.com/bensbe3/pandemic-prediction-system.git
cd pandemic-prediction-system

### Step 2: Install Dependencies
Install Hadoop, Kafka, Spark, PostgreSQL, and required Python packages following the documentation in the configs directory.

### Step 3: Configure the System
Set up configuration files for each component in the configs directory to match your environment.

### Step 4: Initialize the Database
Create the PostgreSQL database and tables needed for storing prediction results.

## Usage

### Step 1: Load Data into HDFS
bash:
./data/load_to_hdfs.sh

### Step 2: Start the Data Pipeline
Start each component in the following order:

Start Zookeeper and Kafka
Create Kafka topic
Start the Kafka producer
Start the Spark consumer
Start the Streamlit visualization

### Step 3: Access the Dashboard
Open your web browser and navigate to:

bash:
http://localhost:8501

#### The dashboard allows you to:

Filter data by country and time period

View trend analyses of pandemic spread

Compare real data with predictions

Explore geographical distribution of cases
