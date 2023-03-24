# Pokebay Project
In this project I created an e-commerce ingestion pipeline from a fake company called Pokebay. <br />
There are three separated dag files each one contatining the production and consumption of a single kafka broker: user, product, profile. <br />
And there is another dag file with the programming of the three brokers together into a single airflow job. <br />
The file that is not a dag contains the functions that the dag calls and has to be located in the same place as the dag using it. 
