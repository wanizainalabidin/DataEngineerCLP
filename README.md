# Data Engineer Case Study for CLP 

This file is divided into Case Study A and Case Study B

As for case study A, the docker file contains environment variables to automtically create containers based on the username and password. I have also created an sql file that will read data from postgres automatically. For this to work, it is necessary for user to initialise building the Postgres container on their own and build connection to SQL file. Below are the steps to create the connection

Build the docker image on terminal
``` 
docker build -t postgresdb .
```
Scan the docker image to see if the recently created image is there 
```
docker images
```
Create the new container in the created image. Here, I'm building the port on 5432, change the port number accordingly if port has been used by other applications
```
docker run -d --name postgresclp -p 5432:5432 postgresdb
```
List down the container
```
docker container ls
```
Execute the recently created container to use it 
```
docker exec it <CONTAINER ID> /bin/bash
```
View what is available inside the container 
```
ls
```
Go into the sql folder that is available in the Docker file
```
cd docker-entrypoint-initdb.d/
```
Use the SQL file available in the same folder
```
cat clp.sql
```
Access the Postgres database with the following command
```
psql -U postgres
```
Access the database 
```
\c clp
```
Ready to use!

The main.py file answers the second part of the case study where users can send a HTTP request to 1) understand data from current database, 2) upload csv file and 3) read uploaded csv which will automatically populate to the current data. For this, I have created a dummy csv file, "clp dummy-2.csv" with different values. 

Lastly, the spark.py file answers the third part of the question which is to process data by removing negative and >100 values for humidity, divide temperature by 100 and calculate the dew point. The output of the file is uploaded in the output folder. IMPT Note: Dew Point values are currently NULL for each row. This is because the format of the file contains either humidity or temperature for each sensor. However, to calculate dew point, both values (temperature and humidity) must be present. 


In case study B, you will see a json file, a python file and an output file. The main.py file is the code that is meant to read data from json and flatten them as data frames. The output file is the saved file in Parquet format. The assumption for this case study is that the Kafka and Spark streaming in python have already been configured in advance and as such, only processing of raw file is required by reading the json file. The folder also includes a word document for the last business question on tools to query data directly from MinIO


