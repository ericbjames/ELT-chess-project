# CHESS_ELT_PROJECT

## Table of Contents 
 - [Introduction](https://github.com/ericbjames/ELT-chess-project/tree/main#Introduction)
 - [Data Source](https://github.com/ericbjames/ELT-chess-project/tree/main#Data-Source)
 - [Solution Architecture](https://github.com/ericbjames/ELT-chess-project/tree/main#Solution-Architecture)
 - [Dimensional Model](https://github.com/ericbjames/ELT-chess-project/tree/main#Dimensional-Model)
 - [Codebase](https://github.com/ericbjames/ELT-chess-project/tree/main#Solution-Architecture)
 - [DBT Lineage](https://github.com/ericbjames/ELT-chess-project/tree/main#DBT-Lineage)

## Introduction
Welcome to my data engineering project where I use the Lichess API to analyze chess games played on their platform. Chess has seen a historic rise in popularity in recent years due to the ease and accessibility of quick online games that allow players to play with opponents from all around the world. One of the most popular forms of online chess is blitz and bullet chess, where games are played with time limits of 3 minutes or less. 

Lichess, a non-profit and open-source online chess platform, has quickly become a favorite among chess enthusiasts due to its commitment to fairness, openness, and transparency. As an open-source platform, Lichess provides an API that allows developers to access a wealth of data about the games played on their platform. 

In this Data Engineering project, I will use the Lichess API to collect, transform, and visualize data on chess games played on the platform. By analyzing this data, I hope to gain insights into the openings used by top players, as well as identify trends and patterns in the game that could be useful in improving player performance.

## Project Architecture
<p align="center">
  <img src=!"Https://github.com/ericbjames/ELT-chess-project/assets/101911329/2e599eda-919b-4b1c-b3e9-6631906e73c6">
</p>


## Data Source/Ingestion
When it comes to getting data from the [Lichess API](https://lichess.org/api), it's crucial to keep in mind the limitations that come with it. These include:

- A throttling limit of only 20 games per second for game data retrieval.
- Only one request can be made at a time, with a 1-minute timeout if this limit is exceeded.
- Computer evaluation data is only accessible if the game has been previously requested to "Run Computer Analysis".

By being aware of these limitations, we can better design our data ingestion process to work within these constraints and avoid running into issues with API usage.

## Dimensional Model

![Screenshot 2023-05-09 at 12 33 14 PM](https://github.com/ericbjames/ELT-chess-project/assets/101911329/653d5bbb-c87f-4022-80dc-81af1e3a2304)

## Codebase

## DBT Lineage
<img width="1297" alt="Screenshot 2023-05-09 at 12 13 52 AM" src="https://github.com/ericbjames/ELT-chess-project/assets/101911329/af6233c0-ca3e-4bb2-91f8-af6e684bbabd">

## Final Dashboard

![Screenshot 2023-05-09 at 12 15 58 AM](https://github.com/ericbjames/ELT-chess-project/assets/101911329/0bb4bfc1-4a63-4f6d-ad45-68ae8d84f19f)



