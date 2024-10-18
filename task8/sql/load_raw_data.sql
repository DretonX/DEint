COPY INTO airline_raw_data
FROM @my_stage/AirlineDataset.csv
FILE_FORMAT = (type = 'CSV', field_delimiter = ',', skip_header = 1)
ON_ERROR = 'CONTINUE';
