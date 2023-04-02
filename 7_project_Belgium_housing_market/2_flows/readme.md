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
prefect deployment build ./etl_gcs_to_bq.py:etl_gcs_to_bq_main --name "Scheduled and parametrized Opendata Belgium GCS to BigQuery" --cron "0 0 1 * *" -a
’’’

### Start worker

’’’
prefect agent start --work-queue "default" 
’’’
