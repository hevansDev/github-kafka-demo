# GitHub Events

This is an updated version of a project I built this project to assist me in my work as a Community Solution Engineer at [TurinTech](https://www.turintech.ai/) by making it easier to research popular repos on GitHub and to report on OKRs.

This project consists of dashboards for visualizing GitHub event data and the associated infrastructure for collecting and serving this data.

## Setup

```bash
git clone https://github.com/hevansDev/github-kafka-demo.git
```

Run the docker compose to deploy the GitHub scraper, Kafka, Clickhouse and Grafana.

```bash
docker compose up -d
```

## Usage

[View Dashboards](http://localhost:3000/)

[View Clickhouse](http://localhost:8123/play)

--- Scratch notes

https://github.com/emanuelef/daily-stars-explorer

isntead of original data source for whole period?

Manually enrich with PRs

https://emanuelef.github.io/daily-stars-explorer/#/apache/kafka

