#!/bin/bash

python pull_reddit_submissions.py
python save_in_mysql.py
python save_simulated_data_in_mysql.py
python retrieve_with_pyspark.py
python data_processing.py
python llm_classification.py
python ml_classifier.py