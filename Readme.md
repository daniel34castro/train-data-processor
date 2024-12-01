## Train Data Processor

## Overview
Train Data Processor is python project processes train data from the Digitraffic API, standardizes and sanitizes it, and sends it to 3 different Kafka topics:
 - rail-raw - topic that holds every record from the api
 - rail-data - processed data after being flattenned. Unique key is composed of trainNumber_scheduledTime 
 - rail-errors - if there is a exception is thrown processing or sending a record, it will be sent to this topic. It holds records that are anomalous (e.g. non-standard date formats, missing values in critical fields, duplicates based on unique identifiers)
 

## Prerequisites
- Docker installed on your system.
- A Kafka cluster available for data ingestion and the credentials to access it. 


## Clone the repository:
git clone https://github.com/your-repo/train-data-processor.git
cd train-data-processor

## Build the Docker image:
docker build -t train-data-processor .

## Running the Docker Container
Use the following command to run the Docker container. Replace the environment variable values with those specific to your Kafka setup:
docker run -e KAFKA_BOOTSTRAP_SERVERS=<your-cluster> \
           -e KAFKA_SASL_USERNAME=<your-kafka-username> \
           -e KAFKA_SASL_PASSWORD=<your-kafka-password> \
           train-data-processor
KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses.
KAFKA_SASL_USERNAME: Kafka SASL username for authentication.
KAFKA_SASL_PASSWORD: Kafka SASL password for authentication.


## Report
The final goal for this challenge was creating a quality report. The architecture of the solution consists on 2 platforms, Confluent Cloud and Elastic Cloud. Confluent cloud hosts the kafka brokers and a connector that sinks data from all the topics to elastic indexes. The report was built in Kibana and it's automatically updated with the elastic indexes.
![image](https://github.com/user-attachments/assets/8e2fb1c4-5a28-4282-86b2-21a47ec5e212)


Potencial improvements
- Retry mechanism in case of failures in Kafka sending (e.g., network issues) 
- Parallel Processing with threadpools or use libraries like Apache Flink or Kafka Streams, which are designed for high-throughput, low-latency processing of large datasets.
- Externalize configuration into a separate config file or enviroment variables


Notes: 
- local file live_trains.json used to test
- Connector uses key for id in elastic
- CSV can be generated in Kibana
- Ignored Arrival types because then unique key would need to be trainNumber_scheduledTime_type
- Data quality of the API is good. Therefore, a method was created to corrupt data and enrich the quality report.
- Credentials elastic cluster: user: elastic; pass: PTb7ERVCwBv51SCbKIlqZGV5
- Elastic index rail-errors, must guarantee scheduledTime has keyword as a mapping. Otherwise, it will be indexed as date from the first document received. When a Invalid Date is received a mapping error will be thrown by the connector. 
PUT _index_template/rail-errors
{
  "index_patterns": [
    "rail-errors*"
  ],
  "template": {
    "mappings": {
      "dynamic_date_formats": [
        "strict_date_optional_time",
        "yyyy/MM/dd HH:mm:ss Z||yyyy/MM/dd Z",
        "basic_date_time",
        "date_optional_time"
      ],
      "_source": {
        "enabled": true
      },
      "dynamic_templates": [],
      "date_detection": true,
      "properties": {
        "record": {
          "type": "object",
          "properties": {
            "timeTableRows": {
              "type": "nested",
              "properties": {
                "scheduledTime": {
                  "type": "keyword"
                }
              }
            }
          }
        },
        "exception": {
          "type": "keyword"
        }
      }
    }
  }
}
