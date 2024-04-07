#!/bin/bash

python pull_reddit_submissions.py
python save_in_mysql.py
# python retrieve_with_pyspark.py
python llm_label_reddit_posts.py
python save_simulated_data_in_mysql.py
# python retrieve_labelled_data_with_pyspark.py
# python data_processing.py  # no need to run this since this is only helper function to clean text which will be called by subsequent scripts
# python llm_classification.py  #no need this script in method 1
# python ml_classifier.py