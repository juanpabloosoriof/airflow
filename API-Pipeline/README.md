# API Connection Flow
In this project I created the following actions:
1. Create an account in https://openweathermap.org/api
2. Generate a token and test the API
3. Use an Airflow HTTP sensor to test the API connection.
4. Use an Airflow python Operator to call the API for 5 cities in the state of Georgia and 5 in the state of California and save the file locally as CSV. 
5. Use an Airflow File Sensor to check if the file landed properly. 
6. Use an Airflow Bash Operator to move the file to a location in the hdfs.
7. Use an Airlfow Spark Submit Operator to execute a Spark job that reads the file from the HDFS, then parses the temperature to Celsius and aggreate the data to get the average and standard deviation per state and save it in the HDFS.
8. Use the Airflow HiveOperator to save the records into a Hive table.
9. Use the Airflow SnsPublishOperator to send an email notification with the status of the job. 
10. schedule the job to run every 10 minutes. 
