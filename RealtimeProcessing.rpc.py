from pyspark import SparkConf, SparkContext
from cryptography.fernet import Fernet
import subprocess
import requests
import tempfile


# Generer én gang og gem nøglen sikkert!
key = Fernet.generate_key()
cipher = Fernet(key)

conf = SparkConf().setAppName("WordCountApp").setMaster("local")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


def encrypt_text(text: str) -> str:
    return cipher.encrypt(text.encode()).decode()

def decrypt_text(text: str) -> str:
    return cipher.decrypt(text.encode()).decode()


def Extract(url, hdfs_path, filename):
    hdfs_file_path = f"{hdfs_path}/{filename}"

    subprocess.run(["hadoop", "fs", "-rm", "-f", hdfs_file_path], check=False)

    response = requests.get(url, stream=True, verify=True)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                tmp_file.write(chunk)
        temp_file_path = tmp_file.name

    subprocess.run(["hadoop", "fs", "-put", "-f", temp_file_path, hdfs_file_path], check=True)

    print(f"Download complete: {filename} -> {hdfs_file_path}")


def Transform(hdfs_input_path, hdfs_output_path, output_filename):
    text_rdd = sc.textFile(hdfs_input_path)

    word_counts_rdd = (
        text_rdd
        .flatMap(lambda line: line.split())
        .map(lambda word: (word.strip('.,?!-:;()[]{}"\'').lower(), 1))
        .filter(lambda x: x[0] != '')
        .reduceByKey(lambda a, b: a + b)
    )

    total_words = word_counts_rdd.map(lambda x: x[1]).sum()

    total_words_encrypted = encrypt_text(str(int(total_words)))
    results_rdd = sc.parallelize([f"Total words: {total_words_encrypted}"])

    results_rdd = results_rdd.union(word_counts_rdd.map(lambda x: f"{x[0]}: {x[1]}"))

    hdfs_output_file = f"{hdfs_output_path}/{output_filename}"
    temp_output_dir = f"{hdfs_output_file}_tmp"

    # Clean up any existing temp directory
    subprocess.run(f"hadoop fs -rm -r {temp_output_dir}", shell=True, check=False)
    # Clean up final output if exists
    subprocess.run(f"hadoop fs -rm -r {hdfs_output_file}", shell=True, check=False)

    results_rdd.coalesce(1).saveAsTextFile(temp_output_dir)

    subprocess.run(f"hadoop fs -mv {temp_output_dir}/part-00000 {hdfs_output_file}", shell=True, check=True)
    subprocess.run(f"hadoop fs -rm -r {temp_output_dir}", shell=True, check=True)

    print(f"Word count complete")

def Load():
    hdfs_output_file = f"{hdfs_output_path}/{output_filename}"
    with subprocess.Popen(["hadoop", "fs", "-cat", hdfs_output_file], stdout=subprocess.PIPE) as proc:
        for line in proc.stdout:
            line = line.decode("utf-8").strip()
            if line.startswith("Total words:"):
                encrypted_part = line.split(":", 1)[1].strip()
                decrypted_total = decrypt_text(encrypted_part)
                print(f"Total words: {decrypted_total}")
            else:
                print(line)


if __name__ == "__main__":
    url = "https://github.com/FilipeLopesPires/LargeText-WordCount/blob/main/datasets/AChristmasCarol_CharlesDickens/AChristmasCarol_CharlesDickens_English.txt"
    hdfs_input_path = "hdfs://localhost:9000/input_dir"
    input_filename = "AChristmasCarol_CharlesDickens_English.txt"
    hdfs_output_path = "hdfs://localhost:9000/output_dir"
    output_filename = "output.txt"
    hdfs_input_file_path = f"{hdfs_input_path}/{input_filename}"
    Extract(url, hdfs_input_path, input_filename)
    Transform(hdfs_input_file_path, hdfs_output_path, output_filename)
    Load()
    sc.stop()

    