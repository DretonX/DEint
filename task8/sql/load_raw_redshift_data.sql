COPY airline_raw_data
FROM 's3://reds3shift/AirlineDataset.csv'
IAM_ROLE 'your_full role'
CSV
IGNOREHEADER 1;