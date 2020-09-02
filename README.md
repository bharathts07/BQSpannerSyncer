## BQSpannerSyncer

Objective : A python apache beam pipeline syncer that copies a BigQuery table to Spanner using apache beam pipeline

### Initial Setup
```shell script
pip3 install pipenv
pipenv install
```

## Run

Run a local job using Apache Beam Direct Runner that will write the results to a local txt file
```shell script
make run
```