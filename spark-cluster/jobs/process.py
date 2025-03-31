from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date
import os

# Яка «середня» тривалість поїздки на день?
def avg_trip_duration(df):
    df = df.withColumn("tripduration", col("tripduration").cast("double"))
    df = df.withColumn("date", to_date(col("start_time")))
    result = df.groupBy("date").agg(avg("tripduration").alias("avg_tripduration"))
    return result # in minutes

# Скільки поїздок було здійснено кожного дня
def trip_count_per_day(df):
    df = df.withColumn("date", to_date(col("start_time")))
    result = df.groupBy("date").count()
    return result

# Яка була найпопулярніша початкова станція для кожного місяця
def most_popular_start_station(df):
    result = df.groupBy("to_station_name").count().orderBy(col("count").desc())
    return result

# Які станції входять у трійку лідерів станцій для поїздок кожного дня протягом останніх двох тижнів
# проте дані лише за 2019 рік
def top_3_stations_per_day_last_2_weeks(df):
    df = df.withColumn("date", to_date(col("start_time")))
    df_filtered = df.where("date > '2019-12-14'")

    result_to_station = df_filtered.groupBy("date", "to_station_name")\
        .count().withColumnRenamed("to_station_name", "station_name")
    result_from_station = df_filtered.groupBy("date", "from_station_name")\
        .count().withColumnRenamed("from_station_name", "station_name")
   
    result = result_to_station.union(result_from_station)\
        .groupBy("date", "station_name").sum("count")\
        .withColumnRenamed("sum(count)", "total_count")\
        .orderBy(col("total_count").desc()).limit(3)
    return result

# Чоловіки чи жінки їздять довше в середньому
def avg_trip_duration_per_gender(df):
    df = df.withColumn("tripduration", col("tripduration").cast("double"))
    result = df.where(col("gender").isNotNull()).groupBy("gender").agg(avg("tripduration").alias("avg_tripduration"))
    return result # in minutes

if __name__ == "__main__":
    master_url = os.getenv("SPARK_MASTER_URL")
    data_csv = os.getenv("DATA_CSV", "/app/data/data.csv")
    print('data_csv:', data_csv)

    spark = SparkSession.builder\
        .appName("process")\
        .master(master_url)\
        .getOrCreate()

    df = spark.read.csv(data_csv, header=True, inferSchema=True)
    df.printSchema()

    print("Яка «середня» тривалість поїздки на день?")
    result1 = avg_trip_duration(df)
    result1.show(truncate=False)

    print("Скільки поїздок було здійснено кожного дня?")
    result2 = trip_count_per_day(df)
    result2.show(truncate=False)

    print("Яка була найпопулярніша початкова станція для кожного місяця?")
    result3 = most_popular_start_station(df)
    result3.show(truncate=False)

    print("Які станції входять у трійку лідерів станцій для поїздок кожного дня протягом останніх двох тижнів?")
    result4 = top_3_stations_per_day_last_2_weeks(df)
    result4.show(truncate=False)

    print("Чоловіки чи жінки їздять довше в середньому?")
    result5 = avg_trip_duration_per_gender(df)
    result5.show(truncate=False)

    # Результати виконання записати у вигляді csv файлу в окремому каталозі з відповідною
    # назвою, які в свою чергу помістити у каталог out
    result1.coalesce(1).write.csv("out/avg_trip_duration", header=True)
    result2.coalesce(1).write.csv("out/trip_count_per_day", header=True)
    result3.coalesce(1).write.csv("out/most_popular_start_station", header=True)
    result4.coalesce(1).write.csv("out/top_3_stations_per_day_last_2_weeks", header=True)
    result5.coalesce(1).write.csv("out/avg_trip_duration_per_gender", header=True)
