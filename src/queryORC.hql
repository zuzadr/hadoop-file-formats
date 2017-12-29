CREATE EXTERNAL TABLE IF NOT EXISTS TaxisORC(VendorID INT,tpep_pickup_datetime BIGINT, tpep_dropoff_datetime BIGINT,
passenger_count INT,trip_distance DOUBLE,RatecodeID INT,store_and_fwd_flag STRING,PULocationID INT,
DOLocationID INT,payment_type INT,fare_amount DOUBLE,extra DOUBLE,mta_tax DOUBLE,tip_amount DOUBLE,
tolls_amount DOUBLE,improvement_surcharge DOUBLE,total_amount DOUBLE)

     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY ','
     LINES TERMINATED BY '\n'
     STORED AS ORC;
     LOCATION '/tmp/inz/HiveORC'


LOAD DATA INPATH 'hdfs://sandbox.hortonworks.com:8020/tmp/inz/yellow_tripdata_2017-06.csv' OVERWRITE INTO TABLE TaxisORC;