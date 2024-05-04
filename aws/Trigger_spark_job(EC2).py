import gzip
import boto3
import argparse
from dotenv import load_dotenv 
import os

class EMRServerless:

    def __init__(self, application_id: str = None) -> None:
        env_path ='/home/ubuntu/python_script/.env'
        load_dotenv(dotenv_path=env_path)
        os.environ['AWS_ACCESS_KEY_ID']=os.getenv('aws_id')
        os.environ['AWS_SECRET_ACCESS_KEY']=os.getenv('aws_key')
        os.environ['AWS_DEFAULT_REGION']='ap-southeast-1'
        self.application_id = '00fh534gvmg2k825'
        self.s3_log_prefix = "emr-serverless-logs"
        self.app_type = "SPARK"
        self.client = boto3.client("emr-serverless")

    def __str__(self):
        return f"EMR Serverless {self.app_type} Application: {self.application_id}"

    def start_application(self, wait: bool = True) -> None:
        """
        Start the application - by default, wait until the application is started.
        """
        if self.application_id is None:
            raise Exception(
                "No application_id - please use creation_application first."
            )

        self.client.start_application(applicationId=self.application_id)

        app_started = False
        while wait and not app_started:
            response = self.client.get_application(applicationId=self.application_id)
            app_started = response.get("application").get("state") == "STARTED"

    def stop_application(self, wait: bool = True) -> None:
        """
        Stop the application - by default, wait until the application is stopped.
        """
        self.client.stop_application(applicationId=self.application_id)

        app_stopped = False
        while wait and not app_stopped:
            response = self.client.get_application(applicationId=self.application_id)
            app_stopped = response.get("application").get("state") == "STOPPED"

    def run_spark_job(
        self,
        script_location: str,
        job_role_arn: str,
        arguments: list,
        s3_bucket_name: str,
        wait: bool = True,
    ) -> str:
        response = self.client.start_job_run(
            name='QiuSong_is_here',
            applicationId=self.application_id,
            executionRoleArn=job_role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": script_location,
                    "entryPointArguments": arguments,
                    "sparkSubmitParameters": "--conf spark.archives=s3://aws-emr-studio-533267180383-ap-southeast-1/python-virtual-env/pyspark_ge.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{s3_bucket_name}/{self.s3_log_prefix}"
                    }
                }
            },
        )
        job_run_id = response.get("jobRunId")

        job_done = False
        while wait and not job_done:
            jr_response = self.get_job_run(job_run_id)
            job_done = jr_response.get("state") in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
            ]

        return job_run_id

    def get_job_run(self, job_run_id: str) -> dict:
        response = self.client.get_job_run(
            applicationId=self.application_id, jobRunId=job_run_id
        )
        return response.get("jobRun")

    def fetch_driver_log(
        self, s3_bucket_name: str, job_run_id: str, log_type: str = "stdout"
    ) -> str:
        """
        Access the specified `log_type` Driver log on S3 and return the full log string.
        """
        s3_client = boto3.client("s3")
        file_location = f"{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}.gz"
        try:
            response = s3_client.get_object(Bucket=s3_bucket_name, Key=file_location)
            file_content = gzip.decompress(response["Body"].read()).decode("utf-8")
        except s3_client.exceptions.NoSuchKey:
            file_content = ""
        return str(file_content)


def parse_args():
    parser = argparse.ArgumentParser()
    required_named = parser.add_argument_group(
        "required named arguments"
    ) 
    required_named.add_argument(
        "arn:aws:iam::533267180383:role/service-role/AmazonEMRStudio_RuntimeRole_1708226364507", help="EMR Serverless IAM Job Role ARN", required=True
    )
    required_named.add_argument(
        "aws-emr-studio-533267180383-ap-southeast-1",
        help="Amazon S3 Bucket to use for logs and job output",
        required=True,
    )
    return parser.parse_args()


if __name__ == "__main__":

    # Create and start a new EMRServerless Spark Application
    emr_serverless = EMRServerless()
    emr_serverless.start_application()

    # Run (and wait for) a Spark job
    print("Submitting new Spark job")
    job_run_id = emr_serverless.run_spark_job(
        script_location="s3://aws-emr-studio-533267180383-ap-southeast-1/script/TextClassifier_v4.py",
        job_role_arn="arn:aws:iam::533267180383:role/service-role/AmazonEMRStudio_RuntimeRole_1708226364507",
        arguments=["s3://aws-emr-studio-533267180383-ap-southeast-1/emr-serverless/output"],
        s3_bucket_name="aws-emr-studio-533267180383-ap-southeast-1",
    )
    job_status = emr_serverless.get_job_run(job_run_id)
    print(f"Job finished: {job_run_id}, status is: {job_status.get('state')}")

    # Fetch and print the logs
    spark_driver_logs = emr_serverless.fetch_driver_log("aws-emr-studio-533267180383-ap-southeast-1", job_run_id)
    print("File output from stdout.gz:\n----\n", spark_driver_logs, "\n----")

    emr_serverless.stop_application()
    print("Done! 👋")
