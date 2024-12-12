from emr_comm_serverless_spark import Session
from pydolphinscheduler.tasks.python import Python
from pydolphinscheduler.core.engine import TaskResult

#创建EMR Serverless。可以手动设置应用 ID，若不设置则默认会获取 spark 应用程序中的第 1 个。
# 创建 EMR Serverless Session，显式设置应用 ID 和资源配置
session_emrserverless = Session(
    application_id='your-application-id-here',  # 设置特定的应用 ID
    logs_s3_path='s3://xxx/xx',
    spark_conf='--conf spark.executor.cores=8 --conf spark.executor.memory=32g --conf spark.driver.cores=4 --conf spark.driver.memory=16g',  # 自定义 Spark 配置
    job_role='arn:aws:iam::your-account-id:role/your-custom-role',  # 自定义 IAM 角色
    dolphin_s3_path='s3://your-custom-dolphin-path/',
    tempfile_s3_path='s3://your-custom-temp-path/',
    python_venv_s3_path='s3://your-custom-python-venv-path/custom_venv.tar.gz'
)

#提交 SQL 语句，执行过程中，会持续打印状态并在任务完成时，打印日志
session_emrserverless.submit_sql("sql-task","SELECT * FROM xxtable LIMIT 10")

#提交脚本文件，spark-test.py 是一个 pysark 或者 pyspark.sql 的程序脚本，执行过程中，会持续打印状态并在任务完成时，打印日志
script_result = session_emrserverless.submit_file("script-task","spark-test.py")

# 提交脚本文件
script_result = session_emrserverless.submit_file("script-task", "spark-test.py")

# 检查任务执行结果
if sql_result.success and script_result.success:
    return TaskResult.success("EMR Serverless tasks completed successfully.")
else:
    failed_tasks = []
    if not sql_result.success:
        failed_tasks.append("SQL task")
    if not script_result.success:
        failed_tasks.append("Script task")
    return TaskResult.failure(f"EMR Serverless tasks failed: {', '.join(failed_tasks)}")
