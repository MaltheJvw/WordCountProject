from pyspark import SparkConf, SparkContext
from hdfs import InsecureClient
import requests
import os 
import glob

conf = SparkConf().setAppName("WordCountApp").setMaster("local")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

client = InsecureClient("http://localhost:9870", user="malthe")

def Extract(url, hdfs_path, filename):
    hdfs_file_path = f"{hdfs_path}/{filename}"

    # Remove existing file if exists 
    if client.status(hdfs_file_path, strict=False):
        client.delete(hdfs_file_path)

    # Stream download adn write to HDFS
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with client.write(hdfs_file_path, encoding='utf-8') as writer:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                writer.write(chunk.decode("utf-8"))

    print(f"Dowloaded {filename} to DHFS at {hdfs_file_path}.")


def Transform(hdfs_input_path, hdfs_output_path, output_filename):
    # Use full HDFS URI so Spark reads via HDFS RPC
    hdfs_uri_input = f"hdfs://localhost:9000{hdfs_input_path}"
    text_rdd = sc.textFile(hdfs_uri_input)

    word_counts_rdd = (
        text_rdd
        .flatMap(lambda line: line.split())
        .map(lambda word: (word.strip('.,?!-:;()[]{}"\'').lower(), 1))
        .filter(lambda x: x[0] != '')
        .reduceByKey(lambda a, b: a + b)
    )

    total_words = word_counts_rdd.map(lambda x: x[1]).sum()
    results_rdd = sc.parallelize([f"Total words: {int(total_words)}"])
    results_rdd = results_rdd.union(word_counts_rdd.map(lambda x: f"{x[0]}: {x[1]}"))

    # Save results to local temp folder
    temp_output_dir = "/tmp/wordcount_temp"
    results_rdd.coalesce(1).saveAsTextFile(temp_output_dir)

    # Move the single part file to HDFS
    part_file = glob.glob(f"{temp_output_dir}/part-*")[0]
    hdfs_output_file = f"{hdfs_output_path}/{output_filename}"

    if client.status(hdfs_output_file, strict=False):
        client.delete(hdfs_output_file)

    with open(part_file, 'r') as local_file, client.write(hdfs_output_file, encoding='utf-8') as writer:
        writer.write(local_file.read())

    # Clean up local temp
    os.system(f"rm -rf {temp_output_dir}")

    print(f"Word count complete.")


def Load(hdfs_output_path, output_filename):
    hdfs_output_file = f"{hdfs_output_path}/{output_filename}"

    with client.read(hdfs_output_file, encoding='utf-8') as reader:
        print(reader.read())
        
    


if __name__ == "__main__":
    url = "https://raw.githubusercontent.com/FilipePires98/LargeText-WordCount/main/datasets/AChristmasCarol_CharlesDickens/AChristmasCarol_CharlesDickens_English.txt"

    hdfs_input_path = "/input_dir"
    input_filename = "AChristmasCarol_CharlesDickens_English.txt"
    hdfs_output_path = "/output_dir"
    output_filename = "output.txt"

    Extract(url, hdfs_input_path, input_filename)
    Transform(hdfs_input_path + "/" + input_filename, hdfs_output_path, output_filename)
    Load(hdfs_output_path, output_filename)

    sc.stop()

