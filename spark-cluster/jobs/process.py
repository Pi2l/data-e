from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date


# Яка «середня» тривалість поїздки на день?
def avg_trip_duration(df):
    df = df.withColumn("tripduration", col("tripduration").cast("double"))
    df = df.withColumn("date", to_date(col("start_time")))
    result = df.groupBy("date").agg(avg("tripduration").alias("avg_tripduration"))
    return result # in minutes

# Скільки поїздок було здійснено кожного дня
def trip_count_per_day(df):
    result = df.groupBy("start_time").count()
    return result

# Яка була найпопулярніша початкова станція для кожного місяця
def most_popular_start_station(df):
    result = df.groupBy("to_station_name").count().orderBy(col("count").desc())
    return result

# Які станції входять у трійку лідерів станцій для поїздок кожного дня протягом останніх двох тижнів
# проте дані лише за 2019 рік
def top_3_stations_per_day_last_2_weeks(df):
    df = df.withColumn("date", to_date(col("start_time")))
    result_to_station = df.where("date > '2019-12-14'").groupBy("to_station_name").count().orderBy(col("count").desc()).limit(3)
    result_from_station = df.where("date > '2019-12-14'").groupBy("from_station_name").count().orderBy(col("count").desc()).limit(3)
    # todo: merge two dataframes that are unique
    result = result_to_station.union(result_from_station).orderBy(col("count").desc()).limit(3)
    return result

# Чоловіки чи жінки їздять довше в середньому
def avg_trip_duration_per_gender(df):
    df = df.withColumn("tripduration", col("tripduration").cast("double"))
    result = df.where(col("'gender is not null'")).groupBy("gender").agg(avr("tripduration").alias("avg_tripduration"))
    return result

if __name__ == "__main__":

    spark = SparkSession.builder.appName("process").getOrCreate()

    df = spark.read.csv("data.csv", header=True, inferSchema=True)
    df.printSchema()

    print("Яка «середня» тривалість поїздки на день?")
    result = avg_trip_duration(df)
    result.show()

    print("Скільки поїздок було здійснено кожного дня?")
    result = trip_count_per_day(df)
    result.show()

    print("Яка була найпопулярніша початкова станція для кожного місяця?")
    result = most_popular_start_station(df)
    result.show()

    print("Які станції входять у трійку лідерів станцій для поїздок кожного дня протягом останніх двох тижнів?")
    result = top_3_stations_per_day_last_2_weeks(df)
    result.show()

    print("Чоловіки чи жінки їздять довше в середньому?")
    result = avg_trip_duration_per_gender(df)
    result.show()