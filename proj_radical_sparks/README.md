# Project Radical Sparks
## Online Extremism Detection Using Big Data & AI

![image](https://github.com/wbcmthh42/bead_pyspark/assets/104043746/b813136a-96ff-4af7-9ecf-90cf3c0c6c6a)

Test Local Inference Script:
![image](https://github.com/wbcmthh42/bead_pyspark/assets/104043746/b7fcc649-7aa6-4a69-bddd-a7749b44d50f)

#### Here are the steps on how to run the scripts in a model training pipeline:

1. The first step is to create a Python script named ``pull_reddit_submissions.py``. This script is responsible for collecting data from Reddit submissions saved in a csv file.

2. The next step is to run a script named ``save_in_mysql.py``. This script saves the data collected from Reddit submissions to a MySQL database.

3. The script named ``save_simulated_data_in_mysql.py`` is then run. This script saves the simulated data, label them as 'positive' radical posts and saves it to the MySQL database.

4. Next, another Python script named ``retrieve_with_pyspark.py`` is likely used to retrieve the data from (2) from the MySQL database using PySpark. The script named ``llm_label_reddit_posts.py`` is then run. This script is responsible for labeling these Reddit posts.

5. To automate the run step 1 to 4, a shell script named ``pipeline_before_human_review.sh`` can also be executed. This script calls other scripts or functions to prepare the data for human review.

6. After the data is labeled, a Jupyter Notebook named ``human_in_loop_review.ipynb`` is used for human review of the labeled data.

7. Once the data is reviewed, a script named ``retrieve_labelled_data_with_pyspark.py`` is used to retrieve the labeled data from the MySQL database using PySpark.

8. After the data is processed, a script named ``data_processing.py`` is used to perform additional data processing tasks like text cleaning etc. This script is called within another script named ``ml_classifier.py``. The script trains a machine learning classifier on the processed data.

9. To automate the run step 6 to 8, a shell script named ``pipeline_after_human_review.sh`` can also be executed these steps together in a pipeline.

10. Finally, the test set is also evaluated to find out the performance of the LLM classifier. This can be run in the script named ``llm_classification.py`` is then run.

#### Here are the steps on how to run the scripts for local inference pipeline (for testing before migration to cloud):

1. The first step is to create a Python script named ``pull_reddit_submissions.py``. This script is responsible for collecting data from new unseen Reddit submissions.

2. The next step is to run a script named ``save_in_mysql.py``. This script saves the data collected from Reddit submissions to a MySQL database.

3. The script named ``retrieve_with_pyspark.py`` and ``data_processing.py`` is then run, and called in the ``llm_label_reddit_posts.py`` to do the inferencing and gives the inference results.

4. To automate the run step 1 to 4, a shell script named ``inference.sh`` can also be executed.
