# CHESS_ELT_PROJECT

## Table of Contents 
 - [Introduction](https://github.com/ericbjames/ELT-chess-project/tree/main#Introduction)
 - [Data Source](https://github.com/ericbjames/ELT-chess-project/tree/main#Data-Source)
 - [Ingestion](https://github.com/ericbjames/ELT-chess-project/tree/main#Ingestion)
 - [Solution Architecture](https://github.com/ericbjames/ELT-chess-project/tree/main#Solution-Architecture)
 - [Dimensional Model](https://github.com/ericbjames/ELT-chess-project/tree/main#Dimensional-Model)
 - [Codebase](https://github.com/ericbjames/ELT-chess-project/tree/main#Codebase)
 - [DBT Lineage](https://github.com/ericbjames/ELT-chess-project/tree/main#DBT-Lineage)

## Introduction
Welcome to my data engineering project where I use the Lichess API to analyze chess games played on their platform. Chess has seen a historic rise in popularity in recent years due to the ease and accessibility of quick online games that allow players to play with opponents from all around the world. One of the most popular forms of online chess is blitz and bullet chess, where games are played with time limits of 3 minutes or less. 

Lichess, a non-profit and open-source online chess platform, has quickly become a favorite among chess enthusiasts due to its commitment to fairness, openness, and transparency. As an open-source platform, Lichess provides an API that allows developers to access a wealth of data about the games played on their platform. 

In this Data Engineering project, I will use the Lichess API to collect, transform, and visualize data on chess games played on the platform. By analyzing this data, I hope to gain insights into the openings used by top players, as well as identify trends and patterns in the game that could be useful in improving player performance.

## Project Architecture
<p align="center">
  <img src="https://github.com/ericbjames/ELT-chess-project/assets/101911329/32c16a08-86b3-4c21-9835-fbc1ebf4ee0b">
</p>


## Data Source
When it comes to getting data from the [Lichess API](https://lichess.org/api), it's crucial to keep in mind the limitations that come with it. These include:

- A throttling limit of only 20 games per second for game data retrieval.
- Only one request can be made at a time, with a 1-minute timeout if this limit is exceeded.
- Computer evaluation data is only accessible if the game has been previously requested to "Run Computer Analysis".

By being aware of these limitations, we can better design our data ingestion process to work within these constraints and avoid running into issues with API usage.

## Ingestion
In the ingestion phase of the project, I've set up a Python script that fetches data from the Lichess API, specifically focusing on bullet, blitz, and ultrabullet games of user 'penguingim1'. The script operates in 21-second intervals (to comply with rate limits) for a duration of 10 minutes. This collected data is loaded into a Kafka topic, which is then transferred into Snowflake via a Confluent connector, setting the stage for dbt modeling and analysis.

## Dimensional Model
<p align="center">
  <img src="https://github.com/ericbjames/ELT-chess-project/assets/101911329/653d5bbb-c87f-4022-80dc-81af1e3a2304">
</p>



## Codebase

### infrastructure/producer
The `infrastructure/producer` folder holds the code for the customer Kafka producer I created for data ingestion. This includes the Dockerfile that I use to host the producer apart from my dbt project on AWS.

### warehouse/snowflake
The `warehouse/snowflake` folder holds the full dbt project which is also dockerized for AWS.


## DBT Lineage
<p align="center">
  <img src="https://github.com/ericbjames/ELT-chess-project/assets/101911329/af6233c0-ca3e-4bb2-91f8-af6e684bbabd">
</p>

## Final Dashboard
Before creating a dashboard ask yourself a few questions:
- What is the primary focus?
- What type of decisions will be made based on the data?
- How detailed does your dashboard need to be?

The primary focus of this particular dashboard is to help the viwer choose new Chess Openings to try out based on what the selected talented player uses.

The completed visualization includes a few graphs:
- Win percentage by chess opening, sorted by play count (Table)
- Chess rating over time
- Win Rate by day
- Top 10 Chess Openings play rate
<p align="center">
  <img src="https://github.com/ericbjames/ELT-chess-project/assets/101911329/0bb4bfc1-4a63-4f6d-ad45-68ae8d84f19f">
</p>



