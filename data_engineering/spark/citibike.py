# Copyright 2022 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https: // www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, year, current_date, count, trim
from pyspark.sql.types import BooleanType

if len(sys.argv) == 1:
    print("Please provide a dataset name.")

dataset = sys.argv[1]
table = "bigquery-public-data:new_york_citibike.citibike_trips"

spark = SparkSession.builder \
    .appName("pyspark-example") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar") \
    .getOrCreate()

df = spark.read.format("bigquery").load(table)



# Crie código com PySpark para calcular a duração média das viagens e exiba as 10 mais lentas.
tripduration_avg = df.agg(avg("tripduration").alias("avg_tripduration")).collect()[0]["avg_tripduration"]
print(f"A duração média das viagens é {tripduration_avg:.3f}s")

df_with_fastest_trips = df.orderBy("tripduration", ascending=False) \
                          .limit(10) \
                          .select("tripduration")
df_with_fastest_trips.show()



# Crie código com PySpark para calcular a duração média das viagens e exiba as 10 mais rápidas (remova as sem duração).
tripduration_avg = df.filter(col("tripduration").isNotNull()) \
                     .agg(avg("tripduration").alias("avg_tripduration")) \
                     .collect()[0]["avg_tripduration"]
print(f"A duração média das viagens é {tripduration_avg:.3f}s")

df_with_slower_trips = df.filter(col("tripduration").isNotNull()) \
                         .orderBy("tripduration", ascending=True) \
                         .limit(10) \
                         .select("tripduration")
df_with_slower_trips.show()



# Crie código com PySpark para calcular a duração média das viagens para cada par de estações. Em seguida,  ordene-as e exiba as 10 mais mais lentas. 
df_grouped_by_stations = df.groupBy("start_station_id", "end_station_id") \
                           .agg(avg("tripduration").alias("duration")) \
                           .orderBy("duration", ascending=False) \
                           .limit(10) \
                           .select("start_station_id", "end_station_id", "duration")
df_grouped_by_stations.show()



# Crie código com PySpark para calcular o total de viagens por bicicleta e exibe as 10 bicicletas mais utilizadas.
df_total_bike_trips = df.filter(col("bikeid").isNotNull()) \
                        .groupBy("bikeid") \
                        .agg(count("*").alias("total_trips")) \
                        .orderBy("total_trips", ascending=False) \
                        .limit(10) \
                        .select("bikeid", "total_trips")
df_total_bike_trips.show()



# Crie código com PySpark para calcular a média de idade dos clientes
avg_client_age = df.filter(col("birth_year").isNotNull()) \
                   .withColumn("age", year(current_date()) - col("birth_year")) \
                   .agg(avg("age").alias("age_avg")) \
                   .collect()[0]["age_avg"]
print(f"A média de idade dos clientes é: {avg_client_age:.2f} anos")



# Crie código com PySpark para calcular a distribuição entre gêneros
df_gender_distribution = df.filter(col("gender").isNotNull() & (trim(col("gender")) != "")) \
                           .groupBy("gender") \
                           .count()
df_gender_distribution.show()


spark.stop()
