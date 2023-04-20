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
Dashboard [link](https://lookerstudio.google.com/reporting/25d568fa-1a4d-4e6c-bc6c-33b9798f2b0b).

### Project folder structure

- 1_infrastructure: contains the Terraform files for initializing the Google Cloud resources.
- 2_flows: contains ETL Prefect scripts and sample deployments.
- 3_transformations: contains a Jupyter Notebook for experimenting with PySpark data analysis and transformation (the current functionality is already implemented in a flow). 

### Create the cloud infrastructure

First you need to create and activate your Google Cloud Platform account. Then create a project using the Web Console (e.g. 'belgian-housing-market').

Also create a Service Account with rights to read and write to storage and BigQuery. Download the JSON access token, it will be useful later.

(optional) Create a Virtual Machine if you are planning to not use your local PC. It is also easier if you configure the VM to have a static external IP. Follow the guide [here](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address). Also make sure you can connect to this VM from a local PC via SSH. This [guide](https://kloudle.com/academy/5-ways-to-connect-to-your-gcp-vm-instances-using-ssh/) provides multiple options for this. Further instructions for running this project only depend slightly on the choice of using a VM instead of the local PC. 

Install Terraform if it is not already installed. Follow the instructions [here](https://developer.hashicorp.com/terraform/downloads). 

Navigate to the 1_infrastructure folder and run 
```
terraform apply
```

If prompted for the project name, write again your project name. 

The Google Cloud infrastructure should now contain empty Storage and BigQuery items.

Navigate back to the project root folder.

### Create the conda environment

Install conda following the instructions from [here](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html).

Create the conda environment from the environment.yml file by running
```
conda env create -f environment.yml
```

and then activate the environment:
```
conda activate dezoomcamp
```

If this step renders any issues, please refer to the conda [documentation](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file).

### Configure Prefect (orchestration framework)

Start the Orion interface ('prefect orion' will change to 'prefect server' starting August 2023):
```
prefect orion start
```

Open the Orion interface in your browser by accessing localhost:4200. If you are using a VM and want to connect to the interface from a local PC, you need to do port forwarding via SSH:
```
ssh -L 4200:localhost:4200 user@remote_ip
```

Two credential blocks need to be created (use the Service Account JSON token generated earlier):
- GCP Credentials: belgium-housing-gcp-cred
- GCS Bucket: belgium-housing-gcp

Note: if you change these names, the Python scripts must be adapted also.

Now you can simply run the ETL pipeline as Python scripts. Navigate to the 2_flows folder and run:
```
python3 etl_web_to_gcs.py
```

```
python3 etl_gcs_to_bq.py
```

```
python3 etl_bq_pyspark_bq.py
```

Note: for the last one, one needs to install Spark on the machine running the script (local or remote VM). To install Spark, follow the guide corresponding to your OS [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_5_batch_processing/setup).

### Create deployments

Running Python scripts is a one time thing and does not really enable the power of orchestration tools like Prefect.

One needs to create deployments by running the following commands from the 2_flows folder.

Create a scheduled (every 1st of each month at 00:00) deployment for extracting the data from the source and uploading to the Google Cloud Storage:
```
prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs_main --name "Scheduled and parametrized Opendata Belgium ETL web to GCS" --cron "0 0 1 * *" -a
```

Create a scheduled (every 2nd of each month at 00:00) deployment for taking the data from Google Cloud Storage and creating BigQuery databases:
```
prefect deployment build ./etl_gcs_to_bq.py:etl_gcs_to_bq_main --name "Scheduled and parametrized Opendata Belgium GCS to BigQuery" --cron "0 0 2 * *" -a
```

Create a scheduled (every 3rd day of each month at 00:00) deployment for taking the BigQuery separated data of leases and rent and creating a new joint dataset and upload it to BigQuery: 
```
prefect deployment build ./etl_bq_pyspark_bq.py:etl_bq_pyspark_bq_main --name "Scheduled and parametrized BigQuery transformation with PySpark" --cron "0 0 3 * *" -a
```

Use the existing deployment files in this repository for reference regarding the deployment parameters.

Once the deployments are created, start the Prefect worker:
```
prefect agent start --work-queue "default" 
```


### Running Jupyter Notebook remotely

Start Jupyter Notebook on the VM:
```
jupyter notebook --no-browser --port=8080
```

Create the port forwarding locally:
```
ssh -L 8080:localhost:8080 user@remote_ip
```

Then connect in the browser to localhost:8080 and use the token generated when starting Jupyter Noteobok remotely.

### Improvement points

BigQuery data partitioning by date and clustering by area code (NIS). 

Better parametrization of ETL Python scripts.

Add check of already processed data in order to avoid redoing the same job every month by the pipeline.

Add check of new data source in the source, instead of always downloading the newest version of everything.

More scalable local storage strategy. 

### Acknowledgements

This project was developed for the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) online course by [DataTalksClub](https://datatalks.club/).

Thank you to the instructors and the organizers for offering an awesome class!   

