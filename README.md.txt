Overview
This repository sets up a Docker environment with Spark (master and worker), and Jupyter Notebook for processing restaurant and weather data. The setup allows you to use Spark for distributed data processing and Jupyter for interactive exploration.

Docker Setup
The docker-compose.yml file defines the following services:

spark-master: The Spark master node.
spark-worker: A Spark worker node.
jupyter: A Jupyter Notebook server for running PySpark code.
Services Configuration:
spark-master:

Image: bitnami/spark:3.5.3
Ports: 8080 (Spark UI), 7077 (Spark Master)
Environment: Set SPARK_MODE=master, SPARK_MASTER_HOST=spark-master
spark-worker:

Image: bitnami/spark:3.5.3
Environment: Set SPARK_MODE=worker, SPARK_MASTER=spark://spark-master:7077
jupyter:

Image: jupyter/pyspark-notebook:latest
Port: 8888 (Jupyter Lab)
Depends on: spark-master
Network:
spark-network: Custom bridge network for communication between services.
Data Processing Example
Restaurant and Weather Data
Load Data:
Load restaurant data (CSV) and weather data (Parquet).

python
Копировать код
restaurants_df = spark.read.csv(restaurant_data_path, header=True, inferSchema=True)
weather_df = spark.read.parquet(weather_data_path)
Missing Coordinates:
Filter restaurants with missing coordinates.

python
Копировать код
missing_coordinates_df = restaurants_df.filter(col("lat").isNull() | col("lng").isNull())
Geocoding:
Use OpenCage Geocoding API to fetch coordinates for missing values.

python
Копировать код
def get_coordinates(address):
    # Call OpenCage API and return latitude, longitude
    return lat, lng
Geohash:
Generate geohashes for restaurant locations.

python
Копировать код
def generate_geohash(latitude, longitude):
    return geohash2.encode(latitude, longitude, precision=4)
Steps to Run
Start Docker Containers:
Run docker-compose up to start the services.

Access Jupyter:
Open http://localhost:8888 in your browser to start using Jupyter Notebooks.

Conclusion
This setup allows for scalable data processing with Spark and interactive analysis using Jupyter. You can extend the code for more complex data processing tasks and explore the power of PySpark with distributed computing.

