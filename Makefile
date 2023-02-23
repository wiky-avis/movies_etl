flake8:
	flake8 --config=.flake8

black:
	black . --config pyproject.toml

isort:
	isort .

linters: isort black flake8

up:
	docker-compose up -d

build:
	docker-compose up -d --build

down:
	docker-compose down -v
