include .env

help:
	@echo "## build			- Build Docker Images (amd64) including its inter-container network."
	@echo "## spinup		- Spinup airflow, postgres, and metabase."
	
airflow_build:
	docker build -t airflow-2.7.2 -f ./docker/Dockerfile.airflow .

airflow_start: airflow_build
	docker compose -f ./docker/docker-compose.yaml --env-file .env up -d
	
