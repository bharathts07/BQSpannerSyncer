.PHONY: dep
dep:
	pip3 install pipenv
	pipenv install --dev

.PHONY: run
run:
	pipenv run python -m template.simple_pipe --output=temp.txt