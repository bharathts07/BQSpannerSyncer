.PHONY: dep
dep:
	pip3 install pipenv
	pipenv install --dev

.PHONY: run
run:
	pipenv run python -m pipeline.bq_to_spanner