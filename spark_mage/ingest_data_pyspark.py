import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def download_taxi_data(*args, **kwargs):
    colors = ['yellow', 'green']
    years = [2019, 2020]
    months = range(1, 13)

    for year in years:
        for month in months:
            for color in colors:
                try:
                    file_name = f"{color}_data/{color}_tripdata_{year}-{month:02}.csv.gz"
                    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02}.csv.gz"

                    os.makedirs(os.path.dirname(file_name), exist_ok=True)
                    os.system(f"wget {url} -O {file_name}")

                except Exception as e:
                    print(f"An error occurred while trying to download the data for {color} taxi, year {year}, month {month:02}: {str(e)}")
   
    #creating the spark sesion
    spark = (
        SparkSession
        .builder
        .appName('Test spark')
        .config("spark.driver.extraClasspath","/usr/share/java/postgresql-42.7.2.jar")
        .getOrCreate()
    )
    kwargs['context']['spark'] = spark

    for color in colors:
        # Read data for each color
        df = spark.read.options(header='true', inferSchema='true', compression='gzip').csv(f"{color}_data/")

        # Define table name based on color
        table_name = f"{color}_trip_data"

        # Write data to PostgreSQL
        df.write.format("jdbc") \
          .options(url="jdbc:postgresql://localhost:5432/ny_taxi",
                   dbtable=f"{table_name}",
                   user="root",
                   password="root",
                   driver="org.postgresql.Driver") \
          .mode('overwrite') \
          .save()
        

    # Stop the Spark session
    spark.stop()
    
if __name__ == "__main__":
    download_taxi_data()
