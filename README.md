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

## Historic Data

- 2011-2025 [GitHub Data Set from Kaggle](https://www.kaggle.com/datasets/github/github-repos) with the following [BigQuery query](./data/big-query.sql)
- 2025-now Stars data export from [Daily Stars explorer](https://emanuelef.github.io/daily-stars-explorer/#/apache/kafka) and [enriched with GitHub API data](./data/2025-today-kafka.csv)



