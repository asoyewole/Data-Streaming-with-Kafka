**Data Streaming with Kafka - Project Summary**

**Overview:**
This project delved into the fundamentals of data streaming processes, employing a technical stack comprising Python, PostgreSQL, the Linux terminal, and Apache Kafka. By utilizing a producer Python script, random weather data was generated within defined ranges, simulating information gathered from weather sensors stationed in Winnipeg and Vancouver. Key weather metrics such as temperature, wind speed, and humidity were included in the generated data, encoded as JSON and transmitted to a Kafka topic. Subsequently, a consumer Python script retrieved and decoded the JSON data, thereafter loading it into a PostgreSQL database. 

**Project Highlights:**
- Utilized producer and consumer scripts written in Python to facilitate data transmission and ingestion.
- Employed Kafka as the messaging platform for data transmission, enhancing scalability and fault tolerance.
- Demonstrated creation of Kafka topics, databases, and database tables programmatically using Python scripts, streamlining the setup process.
- Implemented error handling mechanisms to manage potential issues during script execution.
- Leveraged PostgreSQL for data persistence, setting up separate tables for weather data from Winnipeg and Vancouver to mirror real-world scenarios.

**Execution Workflow:**
- Producer script generated random weather data and transmitted it to the Kafka topic at regular intervals.
- Consumer script retrieved the streaming data from the Kafka topic and loaded it into the respective PostgreSQL tables.
- Scripts were executed from the terminal, with zookeeper and Kafka broker services initiated prior to running the producer and consumer scripts.

**Project Conclusion:**
This project provided a practical demonstration of streaming data processing using Kafka, showcasing seamless integration with Python and PostgreSQL. By successfully loading 134 records into each database table, it underscored the effectiveness of Kafka in facilitating real-time data ingestion and processing. The project files, including scripts and logs, are available in a zipped folder alongside this report.
