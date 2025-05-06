# csv-metrics-reporter
A serverless application built with Flask, deployed on Google Cloud Run, that automatically processes CSV files uploaded to a Google Cloud Storage bucket. Upon detecting a new CSV file, it computes basic metrics (such as row count and null counts per column) and stores the results as a JSON report in a designated folder.
