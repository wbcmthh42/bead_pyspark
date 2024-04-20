#!/bin/bash

python pull_reddit_submissions.py
python save_in_mysql.py
 python llm_label_reddit_posts.py
 python save_simulated_data_in_mysql.py