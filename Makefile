.PHONY: setup up demo down reset logs test test-integration verify-model spark-up spark-down benchmark

setup:
	cp .env.example .env
	pip install -r requirements-local.txt

up:
	docker compose up -d

demo:
	docker compose --profile simulation up -d

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f

test:
	python -m pytest tests/

verify-model:
	python scripts/verify_model.py

spark-up:
	docker compose -f docker-compose.yml -f docker-compose.spark.yml up -d

spark-down:
	docker compose -f docker-compose.yml -f docker-compose.spark.yml down
