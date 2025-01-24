CREATE TABLE taxi_data (
    VendorID INT, 
    tpep_pickup_datetime TIMESTAMP, 
    tpep_dropoff_datetime TIMESTAMP, 
    passenger_count INT, 
    trip_distance FLOAT, 
    RatecodeID INT, 
    store_and_fwd_flag CHAR(1), 
    PULocationID INT, 
    DOLocationID INT, 
    payment_type INT, 
    fare_amount FLOAT, 
    extra FLOAT, 
    mta_tax FLOAT, 
    tip_amount FLOAT, 
    tolls_amount FLOAT, 
    improvement_surcharge FLOAT, 
    total_amount FLOAT, 
    congestion_surcharge FLOAT 
);

COPY taxi_data FROM '/data/yellow_tripdata.csv' 
DELIMITER ',' CSV HEADER;


CREATE TABLE taxi_zone_lookup (
    LocationID INT, 
    Borough VARCHAR(50), 
    Zone VARCHAR(100), 
    service_zone VARCHAR(50)
);

COPY taxi_zone_lookup FROM '/data/taxi_zone.csv' 
DELIMITER ',' CSV HEADER;

