## Create needed stufff for spanner

```shell script
 pipenv install --skip-lock google-cloud-storage
```


## Run

```shell script
export GOOGLE_APPLICATION_CREDENTIALS=<PATH TO JSON CREDS>
pipenv run python -m spanner.create
pipenv run python -m spanner.insert
```

For requirements.txt
```shell script
pipenv sync
pipenv run pip freeze
```