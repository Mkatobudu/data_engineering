
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_data
    --url=${URL}


URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_data \
    --url=$URL

 sudo docker build -t taxi_ingest:v001 .


 
 URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

 sudo docker run -it taxi_ingest:v001 \
      --user=root \
      --password=root \
      --host=localhost \
      --port=5432 \
      --db=ny_taxi \
      --table_name=yellow_taxi_data \
      --url=$URL


  URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"


URL="http://192.168.43.6:8000/yellow_tripdata_2021-01.csv.gz"
sudo docker run -it \
       --network=pg-network \
       taxi_ingest:v001 \
         --user=root \
         --password=root \
         --host=pg-database \
         --port=5432 \
         --db=ny_taxi \
         --table_name=yellow_taxi_data \
         --url=$URL


http://192.168.43.6:8000/yellow_tripdata_2021-01.csv.gz
Aa