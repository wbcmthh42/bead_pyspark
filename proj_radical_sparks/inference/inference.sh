#!/bin/bash

python pull_reddit_submissions.py
python save_in_mysql.py
python inference.py
