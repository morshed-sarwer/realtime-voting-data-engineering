# Realtime Election Voting System

This repository contains the code for a realtime election voting system. The system is built using Python, Kafka, Spark Streaming, Postgres, Streamlit, and Docker Compose for orchestration.

![system_architecture](https://github.com/morshed-sarwer/realtime-voting-data-engineering/assets/136965644/a3e72518-997a-4e36-ac9c-b295a49380d7)


## System Components

### `main.py`
- **Description:** Main Python script responsible for:
  - Creating required tables on Postgres (candidates, voters, and votes).
  - Creating the Kafka topic.
  - Copying the votes table to the Kafka topic.
  - Producing data to the `voter_topics` on Kafka.

### `voting.py`
- **Description:** Python script containing logic for:
  - Consuming votes from the Kafka topic (`voter_topics`).
  - Generating voting data.
  - Producing data to the `vote_topics` on Kafka.

### `vote-streaming.py`
- **Description:** Python script containing logic for:
  - Consuming votes from the Kafka topic (`vote_topics`).
  - Enriching data from Postgres.
  - Aggregating the votes.
  - Producing data to the ( `votes_per_candidate`, `turnout_by_location` ) on Kafka.

### `streamlit-app.py`
- **Description:** Python script containing logic for:
  - Consuming aggregated voting data from the Kafka topic.
  - Displaying the voting data in real-time using Streamlit.

## Technologies Used

- **Python:** The primary programming language for scripting.
- **Kafka:** Messaging system for data streaming.
- **Spark Streaming:** Framework for real-time data processing.
- **Postgres:** Relational database for storing candidates, voters, and votes data.
- **Streamlit:** Web app framework for displaying real-time voting data.
- **Docker Compose:** Orchestration tool for containerized applications.
