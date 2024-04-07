#!/bin/bash

# python retrieve_labelled_data_with_pyspark.py
# python data_processing.py  # no need to run this since this is only helper function to clean text which will be called by subsequent scripts
python ml_classifier.py
python llm_classification.py  #no need this script in method 1