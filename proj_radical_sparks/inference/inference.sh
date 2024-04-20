#!/bin/bash

python pull_reddit_submissions.py
python save_in_mysql.py
# streamlit run injects_streamlit_simulated_radical_posts_inference.py
python injects_simulated_radical_posts_inference.py
python inference.py
