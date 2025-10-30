#!/bin/bash

# Activate virtual environment
source ~/Documents/Developer/VSCODE/WordCount/batch/venv/bin/activate

# Step 1: Fetch and prepare data
echo "[STEP 1] Fetching and preparing data..."
python3 ~/Documents/Developer/VSCODE/WordCount/batch/extract.py

# Step 2: Clean up previous output if it exists
echo "[STEP 2] Cleaning up previous output if it exists..."
hdfs dfs -rm -r /output_dir || true

# Step 3: Run MapReduce job
echo "[STEP 3] Running MapReduce job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar \
  -input /input_dir \
  -output /output_dir \
  -mapper "python3 ~/Documents/Developer/VSCODE/WordCount/batch/mapper.py" \
  -reducer "python3 ~/Documents/Developer/VSCODE/WordCount/batch/reducer.py"

