### Dashboard

![alt text](https://github.com/daniel-gheorghita/dezoomcamp/blob/main/7_project_Belgium_housing_market/dashboard.png)

### Start remote servers

on VM: 
jupyter notebook --no-browser --port=8080

local:
ssh -L 8080:localhost:8080 user@remote_ip

local (Prefect):
ssh -L 4200:localhost:4200 user@remote_ip


### Start the Prefect Orion interface

’’’
prefect orion start
’’’

### Create the deployment scheduled every 1st day of the month at 00:00

’’’
prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs_main --name "Scheduled and parametrized Opendata Belgium ETL web to GCS" --cron "0 0 1 * *" -a
’’’

### Create the deployment scheduled every 2nd day of the month at 00:00

’’’
prefect deployment build ./etl_gcs_to_bq.py:etl_gcs_to_bq_main --name "Scheduled and parametrized Opendata Belgium GCS to BigQuery" --cron "0 0 2 * *" -a
’’’

### Create the deployment scheduled every 3rd day of the month at 00:00

’’’
prefect deployment build ./etl_bq_pyspark_bq.py:etl_bq_pyspark_bq_main --name "Scheduled and parametrized BigQuery transformation with PySpark" --cron "0 0 3 * *" -a
’’’

### Start worker

’’’
prefect agent start --work-queue "default" 
’’’

