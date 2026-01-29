from google.cloud import bigquery

client = bigquery.Client()
datasets = list(client.list_datasets())

print("BigQuery access works!")
