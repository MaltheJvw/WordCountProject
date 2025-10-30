# extract.py
from hdfs import InsecureClient
import requests

# Connect to HDFS
client = InsecureClient("http://localhost:9870", user="malthe")

# URL of the data
url = "https://raw.githubusercontent.com/FilipePires98/LargeText-WordCount/main/datasets/AChristmasCarol_CharlesDickens/AChristmasCarol_CharlesDickens_English.txt"

# HDFS target path
hdfs_path = "/input_dir/AChristmasCarol_CharlesDickens_English.txt"

# Delete if exists
if client.status(hdfs_path, strict=False):
    client.delete(hdfs_path)

# Stream data from the web and write directly to HDFS
with requests.get(url, stream=True) as r:
    r.raise_for_status()  # Raise exception for HTTP errors
    with client.write(hdfs_path, encoding='utf-8') as writer:
        for line in r.iter_lines(decode_unicode=True):
            writer.write(line + '\n')

print(f"File successfully written to HDFS at {hdfs_path}")