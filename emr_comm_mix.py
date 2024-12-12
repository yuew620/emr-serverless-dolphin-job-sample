import gzip
import os
from string import Template
import time
import boto3
from datetime import datetime

# this py is for submit job to emr on ec2 and emr serverless 

# EMRResult 类用于存储 EMR 作业的运行 ID 和状态
class EMRResult:
    def __init__(self,job_run_id,status):
        self.job_run_id=job_run_id
        self.status=status

# Session 类用于管理 EMR on EC2 和 EMR Serverless 作业的提交和执行
class Session:
    def __init__(self,
                 application_id='', # 设置 EMR 集群 ID 或 EMR Serverless 应用 ID
                 jobtype=0, # 0: EMR on EC2, 1: EMR Serverless
                 job_role='arn:aws:iam::******:role/AmazonEMR-ExecutionRole-1694412227712',
                 dolphin_s3_path='s3://*****/dolphinscheduler/ec2-user/resources/',
                 logs_s3_path='s3://aws-logs-****-ap-southeast-1/elasticmapreduce/',
                 tempfile_s3_path='s3://****/tmp/',
                 python_venv_s3_path='s3://****/python/pyspark_venv.tar.gz',
                 spark_conf='--conf spark.executor.cores=4 --conf spark.executor.memory=16g --conf spark.driver.cores=4 --conf spark.driver.memory=16g'
                 ):

        self.jobtype=jobtype
        self.application_id = application_id

        self.region='ap-southeast-1'
        self.job_role = job_role
        self.dolphin_s3_path = dolphin_s3_path
        self.logs_s3_path=logs_s3_path
        self.tempfile_s3_path=tempfile_s3_path
        self.spark_conf=spark_conf
        self.python_venv_s3_path=python_venv_s3_path

        self.client = boto3.client('emr', region_name=self.region)
        self.client_serverless = boto3.client('emr-serverless', region_name=self.region)

        # 如果未设置 application_id,则查询当前第一个 active 的 EMR 集群或 EMR Serverless 应用的 ID
        if self.application_id == '':
            self.application_id=self.getDefaultApplicaitonId()

        if jobtype == 0 :  # EMR on EC2
            self.session=EmrSession(
                region=self.region,
                application_id=self.application_id,
                job_role=self.job_role,
                dolphin_s3_path=self.dolphin_s3_path,
                logs_s3_path=self.logs_s3_path,
                tempfile_s3_path=self.tempfile_s3_path,
                python_venv_s3_path=self.python_venv_s3_path,
                spark_conf=self.spark_conf
            )
        elif jobtype ==1 : # EMR Serverless
            self.session=EmrServerlessSession(
                region=self.region,
                application_id=self.application_id,
                job_role=self.job_role,
                dolphin_s3_path=self.dolphin_s3_path,
                logs_s3_path=self.logs_s3_path,
                tempfile_s3_path=self.tempfile_s3_path,
                python_venv_s3_path=self.python_venv_s3_path,
                spark_conf=self.spark_conf
            )
        else: # Pyhive, used on-premise
            self.session=PyHiveSession(
                host_ip="172.31.25.171",
                port=10000
            )

        self.initTemplateSQLFile()

    # 提交 SQL 作业
    def submit_sql(self,jobname, sql):
        result= self.session.submit_sql(jobname,sql)
        if result.status == "FAILED" :
            raise Exception("ERROR：任务失败")

    # 提交文件作业
    def submit_file(self,jobname, filename):
        result=  self.session.submit_file(jobname,filename)
        if result.status == "FAILED":
            raise Exception("ERROR：任务失败")

    # 获取默认的 EMR 集群或 EMR Serverless 应用 ID
    def getDefaultApplicaitonId(self):
        if self.jobtype == 0: # EMR on EC2
            emr_clusters = self.client.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
            if emr_clusters['Clusters']:
                app_id= emr_clusters['Clusters'][0]['Id']
                print(f"选择默认的集群(或EMR Serverless 的应用程序)ID:{app_id}")
                return app_id
            else:
                raise Exception("没有找到活跃的EMR集群")
        elif self.jobtype == 1: # EMR Serverless
            emr_applications = self.client_serverless.list_applications()
            spark_applications = [app for app in emr_applications['applications'] if app['type'] == 'Spark']
            if spark_applications:
                app_id = spark_applications[0]['id']
                print(f"选择默认的应用ID:{app_id}")
                return app_id
            else:
                raise Exception("没有找到活跃的 EMR Serverless 应用")

    # 初始化 SQL 模板文件
    def initTemplateSQLFile(self):
        with open('sql_template.py', 'w') as f:
            f.write('''
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.enableHiveSupport()
    .appName("Python Spark SQL basic example")
    .getOrCreate()
)

df = spark.sql("$query")
df.show()
        ''')

# EMR on EC2 作业提交类
class EmrSession:
    def __init__(self,
                 region,
                 application_id,  #若是EMR on EC2,则设置集群 ID；若不设置，则自动其第一个active的 app 或者cluster
                 job_role,
                 dolphin_s3_path,
                 logs_s3_path,
                 tempfile_s3_path,
                 python_venv_s3_path,
                 spark_conf
                 ):
        self.s3_client = boto3.client("s3")
        self.region=region
        self.client = boto3.client('emr', region_name=self.region)
        self.application_id = application_id

        self.job_role = job_role
        self.dolphin_s3_path = dolphin_s3_path
        self.logs_s3_path=logs_s3_path
        self.tempfile_s3_path=tempfile_s3_path
        self.python_venv_s3_path=python_venv_s3_path
        self.spark_conf=spark_conf

        self.client.modify_cluster(
            ClusterId=self.application_id,
            StepConcurrencyLevel=256
        )

    # 提交 SQL 作业到 EMR on EC2
    def submit_sql(self,jobname, sql):
        # 将 SQL 写入临时文件
        print(f"RUN SQL:{sql}")
        self.python_venv_conf=''
        with open(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql_template.py")
        ) as f:
            query_file = Template(f.read()).substitute(query=sql.replace('"', '\\"'))

            script_bucket = self.tempfile_s3_path.split('/')[2]
            script_key = '/'.join(self.tempfile_s3_path.split('/')[3:])

            current_time = datetime.now().strftime("%Y%m%d%H%M%S")
            script_key = script_key+"sql_template_"+current_time+".py"
            self.s3_client.put_object(
                Body=query_file, Bucket=script_bucket, Key=script_key
            )

            script_file=f"s3://{script_bucket}/{script_key}"
            result= self._submit_job_emr(jobname, script_file)
            self.s3_client.delete_object(
                Bucket=script_bucket, Key=script_key
            )
            return result

    # 提交文件作业到 EMR on EC2  
    def submit_file(self,jobname, filename):
        print(f"Run File :{filename}")
        self.python_venv_conf=''
        if self.python_venv_s3_path and self.python_venv_s3_path != '':
            self.python_venv_conf = f"--conf spark.yarn.dist.archives={self.python_venv_s3_path}#environment --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"

        script_file=f"{self.dolphin_s3_path}{filename}"
        result= self._submit_job_emr(jobname, script_file)

        return result

    # 内部方法,用于提交 EMR on EC2 作业
    def _submit_job_emr(self, jobname, script_file):
        spark_conf_args = self.spark_conf.split()

        # 设置虚拟环境的地址,用于支持 pyspark 以外的库
        python_venv_args=[]
        if self.python_venv_conf and self.python_venv_conf != '':
            python_venv_args=self.python_venv_conf.split()

        jobconfig=[
            {
                'Name': f"{jobname}",
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                                'spark-submit',
                                '--deploy-mode',
                                'cluster',
                                '--master',
                                'yarn',
                                '--conf',
                                'spark.yarn.submit.waitAppCompletion=true'
                            ] + spark_conf_args + python_venv_args + [script_file]
                }
            }
        ]
        response = self.client.add_job_flow_steps(
            JobFlowId=self.application_id,
            Steps=jobconfig
        )
        print(jobconfig)

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            print('task failed：')
            print(response)

        job_run_id = response['StepIds'][0]
        print(f"Submit job on EMR ,job id: {job_run_id}")
        job_done = False
        status='PENDING'
        while not job_done:
            status = self.get_job_run(job_run_id)
            print(f"current status:{status}")
            job_done = status in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
                "COMPLETED"
            ]
            time.sleep(10)

        if status == "FAILED":
            self.print_driver_log(job_run_id,log_type="stderr")
            self.print_driver_log(job_run_id,log_type="stdout")
        return EMRResult(job_run_id,status)

    # 获取 EMR on EC2 作业运行状态
    def get_job_run(self, job_run_id: str) -> dict:
        step_status = self.client.describe_step(
            ClusterId=self.application_id,
            StepId=job_run_id
        )['Step']['Status']['State']
        return step_status.upper()

    # 打印 EMR on EC2 作业的驱动程序日志
    def print_driver_log(self, job_run_id: str, log_type: str = "stderr") -> str:

        print("starting download the driver logs")

        s3_client = boto3.client("s3")
        logs_location = f"{self.logs_s3_path}{self.application_id}/steps/{job_run_id}/{log_type}.gz"
        logs_bucket = logs_location.split('/')[2]
        logs_key = '/'.join(logs_location.split('/')[3:])
        print(f"Fetching {log_type} from {logs_location}")
        try:
            # 日志生成需要一段时间,最长 100 秒
            for _ in range(10):
                try:
                    s3_client.head_object(Bucket=logs_bucket, Key=logs_key)
                    break
                except Exception:
                    print("等待日志生成中...")
                    time.sleep(10)
            response = s3_client.get_object(Bucket=logs_bucket, Key=logs_key)
            file_content = gzip.decompress(response["Body"].read()).decode("utf-8")
        except s3_client.exceptions.NoSuchKey:
            file_content = ""
            print( f"等待超时，请稍后到 EMR 集群的步骤中查看错误日志或者手动前往: {logs_location} 下载")
        print(file_content)

# EMR Serverless 作业提交类
class EmrServerlessSession:
    def __init__(self,
                 region,
                 application_id, #若是 serverless, 则设置 应用的 ID；若不设置，则自动其第一个active的 app 
                 job_role,
                 dolphin_s3_path,
                 logs_s3_path,
                 tempfile_s3_path,
                 python_venv_s3_path,
                 spark_conf
                 ):
        self.s3_client = boto3.client("s3")
        self.region=region
        self.client = boto3.client('emr-serverless', region_name=self.region)
        self.application_id = application_id

        self.job_role = job_role
        self.dolphin_s3_path = dolphin_s3_path
        self.logs_s3_path=logs_s3_path
        self.tempfile_s3_path=tempfile_s3_path
        self.python_venv_s3_path=python_venv_s3_path
        self.spark_conf=spark_conf

    # 提交 SQL 作业到 EMR Serverless
    def submit_sql(self,jobname, sql):
        # 将 SQL 写入临时文件
        print(f"RUN SQL:{sql}")
        self.python_venv_conf=''
        with open(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql_template.py")
        ) as f:
            query_file = Template(f.read()).substitute(query=sql.replace('"', '\\"'))

            script_bucket = self.tempfile_s3_path.split('/')[2]
            script_key = '/'.join(self.tempfile_s3_path.split('/')[3:])

            current_time = datetime.now().strftime("%Y%m%d%H%M%S")
            script_key = script_key+"sql_template_"+current_time+".py"
            self.s3_client.put_object(
                Body=query_file, Bucket=script_bucket, Key=script_key
            )

            script_file=f"s3://{script_bucket}/{script_key}"
            result= self._submit_job_emr(jobname, script_file)

            #delete the temp file
            self.s3_client.delete_object(
                Bucket=script_bucket, Key=script_key
            )
            return result
    def submit_file(self,jobname, filename):  #serverless
        # temporary file for the sql parameter
        print(f"RUN Script :{filename}")

        self.python_venv_conf=''
        if self.python_venv_s3_path and self.python_venv_s3_path != '':
            self.python_venv_conf = f"--conf spark.archives={self.python_venv_s3_path}#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"


        script_file=f"{self.dolphin_s3_path}{filename}"
        result= self._submit_job_emr(jobname, script_file)

        return result


    def _submit_job_emr(self, name, script_file):#serverless
        job_driver = {
            "sparkSubmit": {
                "entryPoint": f"{script_file}",
                "sparkSubmitParameters": f"{self.spark_conf} --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory {self.python_venv_conf}",
            }
        }
        print(f"job_driver:{job_driver}")
        response = self.client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=self.job_role,
            name=name,
            jobDriver=job_driver,
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": self.logs_s3_path,
                    }
                }
            },
        )

        job_run_id = response.get("jobRunId")
        print(f"Emr Serverless Job submitted, job id: {job_run_id}")

        job_done = False
        status="PENDING"
        while not job_done:
            status = self.get_job_run(job_run_id).get("state")
            print(f"current status:{status}")
            job_done = status in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
            ]

            time.sleep(10)

        if status == "FAILED":
            self.print_driver_log(job_run_id,log_type="stderr")
            self.print_driver_log(job_run_id,log_type="stdout")
            raise Exception(f"EMR Serverless job failed:{job_run_id}")
        return EMRResult(job_run_id,status)


    def get_job_run(self, job_run_id: str) -> dict:
        response = self.client.get_job_run(
            applicationId=self.application_id, jobRunId=job_run_id
        )
        return response.get("jobRun")

    def print_driver_log(self, job_run_id: str, log_type: str = "stderr") -> str:


        s3_client = boto3.client("s3")
        logs_location = f"{self.logs_s3_path}applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}.gz"
        logs_bucket = logs_location.split('/')[2]
        logs_key = '/'.join(logs_location.split('/')[3:])
        print(f"Fetching {log_type} from {logs_location}")
        try:
            response = s3_client.get_object(Bucket=logs_bucket, Key=logs_key)
            file_content = gzip.decompress(response["Body"].read()).decode("utf-8")
        except Exception:
            file_content = ""
        print(file_content)