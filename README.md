# README

# Context

The purpose of this workshop is to analyze and clean 5 CSV files containing data on the happiness of different countries from 2015 to 2019. Some of the columns included will be the country name, GDP, trust in government, life expectancy, among others.

The process will begin with the creation of a notebook for conducting Exploratory Data Analysis (EDA) on the CSV files and merging all datasets to work with a single one. In another notebook, the entire process of creating a machine learning model will take place, which should predict a country's happiness. However, this will be preceded by testing and training the model.

The final step will involve streaming the data using Apache Kafka. This consists of 2 Python scripts: one for the producer, which handles all the EDA of the data and sends it via Kafka through the topic “happinessPredictions,” and another for the consumer, which will receive all this data, perform happiness predictions for each row, and finally upload the results to the database.

---

## Tools used

- Python
- Jupyter notebook
- CSV files
- Postgresql
- Kafka
- Docker
- Sklearn

---

## Steps to follow to replicate the process

1. clone the repository:

```bash
git clone https://github.com/2004santiago/Workshop_3.git
```

1. make sure you have Postgresql and create the .env file to save the credentials:

.env

```bash
LOCALHOST=
PORT=
DB_NAME=
DB_USER=
DB_PASS=
```

1. we install the necessary dependencies:

```bash
poetry install
```

on windows: 

```bash
python3 -m poetry install
```

1. we activate docker with:

```bash
docker-compose up 
```

and then enter to the bash:

```bash
docker exec -it kafka-test bash  
```

after we created the topic:

```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic happinessPredictions
```

and we go out the bash with:

```bash
exit
```

1. We run the python scripts 

```bash
python ./kafka/process_data.py
```

```bash
python ./kafka/consumer.py
```

---

Finally we will be able to see the table in our database and observe the data that were predicted