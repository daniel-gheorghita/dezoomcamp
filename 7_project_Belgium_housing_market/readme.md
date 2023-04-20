### Project idea

The housing market can be confusing. Should one buy or rent a house or an apartment? 

This project aims at helping one make the decision by showing the evolution of rents and prices over time in a dashboard, as well as the lowest and highest ratio between price and rent for a selected region (or multiple regions).

The evolution of prices can be helpful to determine if an acquisition is a good investment. 

The ratio of price over rent can be interpreted as: "how many monthly payments (each equivalent to the average rent amount) would I need to pay to buy an average-priced accommodation?". For example, if the ratio is 60, it means that 5 years of rents would equivalate to the price of the accommodation. Of course, this is a naive calculation and it does not take into account inflation, indexing or other socio-economic variables. But it does give a starting point.  

For now, the pipeline only covers The Kingdom of Belgium (or simply Belgium, Europe). The data source is the official open data published by the Federal Public Service Finance. The data is analyzed at municipality (commune) division level.   


### Project structure
![alt text](https://github.com/daniel-gheorghita/dezoomcamp/blob/main/7_project_Belgium_housing_market/project_structure_diagram_white_bg.png)


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

