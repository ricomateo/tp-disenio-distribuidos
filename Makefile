.PHONY: all build up down logs clean client test

REPLICAS ?= 2
COMPOSE_NORMAL = -f docker-compose.yaml
COMPOSE_2 = -f docker-compose-2.yaml
COMPOSE_134 = -f docker-compose-134.yaml
COMPOSE_5 = -f docker-compose-5.yaml
COMPOSE_TEST = -f docker-compose-test.yaml
COMPOSE_GENERATED = -f docker-compose-gen.yaml
PYTHON = python3
GENERATOR_SCRIPT = generador_compose.py
COMPARE_SCRIPT = testing/compare_results.py
JUPYTER_NOTEBOOK = FIUBA_Distribuidos_1_The_Movies.ipynb


all: build up

data/credits.csv:
	@echo "Checking for unrar installation..."
	@which unrar >/dev/null 2>&1 || (echo "Installing unrar..."; sudo apt-get update && sudo apt-get install -y unrar || (echo "Error: Failed to install unrar. Please install it manually with 'sudo apt-get install unrar'."; exit 1))
	@echo "data/credits.csv does not exist. Unrarring data/credits.rar..."
	@if [ ! -f data/credits.rar ]; then \
		echo "Error: data/credits.rar not found"; \
		exit 1; \
	fi
	@unrar x -y data/credits.rar data/ || (echo "Error: Failed to unrar data/credits.rar."; exit 1)

validate-replicas:
	@if [ $(REPLICAS) -lt 1 ]; then \
		echo "Error: REPLICAS must be at least 1"; \
		exit 1; \
	fi

ensure-line-endings:
	@which dos2unix > /dev/null && dos2unix $(GENERATOR_SCRIPT) || true
	@sed -i 's/\r$$//' $(GENERATOR_SCRIPT) || true

generate-compose: ensure-line-endings
	@echo "Generating docker-compose.yaml with the config.ini as configuration."
	$(PYTHON) ./$(GENERATOR_SCRIPT)

build:
	@echo "Building Docker images..."
	docker-compose $(COMPOSE_NORMAL) build

up: generate-compose data/credits.csv
	docker-compose $(COMPOSE_GENERATED) up -d --build 

134: validate-replicas
	docker-compose $(COMPOSE_134) up -d --build \
		--scale parser=$(REPLICAS) \
        --scale filter_argentina_2000=$(REPLICAS) \
        --scale filter_spain_2000s=$(REPLICAS) \
        --scale router=$(REPLICAS) 

5: validate-replicas
	docker-compose $(COMPOSE_5) up -d --build \
		--scale parser=$(REPLICAS) \
		--scale sentiment=$(REPLICAS)

2: validate-replicas
	docker-compose $(COMPOSE_2) up -d --build \
		--scale parser=$(REPLICAS) \
        --scale unique_country=$(REPLICAS) \
        --scale router=$(REPLICAS) 

test: up
	@$(PYTHON) $(COMPARE_SCRIPT) testing/expected_output.txt

down:
	@echo "Stopping services..."
	docker-compose $(COMPOSE_NORMAL) down
	docker-compose $(COMPOSE_TEST) down

logs:
	@echo "Showing logs..."
	docker-compose $(COMPOSE_NORMAL) logs -f

clean:
	@echo "Cleaning up..."
	docker-compose $(COMPOSE_NORMAL) down -v --rmi all --remove-orphans
	docker-compose $(COMPOSE_TEST) down -v --rmi all --remove-orphans
	rm -rf __pycache__ *.pyc src/__pycache__

client:
	@echo "Running client..."
	docker-compose $(COMPOSE_NORMAL) run --rm client

compare-original:
	@echo "Comparing with the output of the kaggle version..."
	$(PYTHON) testing/output_adapter.py output/results.txt testing/received_output.txt
	$(PYTHON) ./$(COMPARE_SCRIPT) testing/expected_output.txt testing/received_output.txt

compare-filtered:
	@echo "Comparing with the output of the filtered version..."
	$(PYTHON) testing/output_adapter.py output/results.txt testing/received_output.txt
	$(PYTHON) ./$(COMPARE_SCRIPT) testing/expected_filtered_output.txt testing/received_output.txt

ensure-results-consistency-2:
	@echo "Comparing the results obtained in the different clients for the amount of 2..."
	$(PYTHON) testing/output_adapter.py output/results.txt testing/received_output.txt
	$(PYTHON) testing/output_adapter.py output/results_1.txt testing/received_output_1.txt
	$(PYTHON) ./$(COMPARE_SCRIPT) testing/received_output.txt testing/received_output_1.txt

ensure-results-consistency-3:
	@echo "Comparing the results obtained in the different clients for the amount of 3..."
	$(PYTHON) testing/output_adapter.py output/results.txt testing/received_output.txt
	$(PYTHON) testing/output_adapter.py output/results_1.txt testing/received_output_1.txt
	$(PYTHON) testing/output_adapter.py output/results_2.txt testing/received_output_2.txt
	$(PYTHON) ./$(COMPARE_SCRIPT) testing/received_output.txt testing/received_output_1.txt
	$(PYTHON) ./$(COMPARE_SCRIPT) testing/received_output.txt testing/received_output_2.txt

clear:
	docker-compose down
	docker system prune -f
	docker network prune -f

jupyter_results: 
	@docker build -f Dockerfile.test -t run-notebook .
	@docker run -it --rm \
		-v $(PWD)/output:/src/output/ \
		-v $(PWD)/config.ini:/src/config.ini \
		-v $(PWD)/data:/src/data -v \
		$(PWD)/$(JUPYTER_NOTEBOOK):/src/$(JUPYTER_NOTEBOOK) run-notebook

test_against_notebook: up jupyter_results
	$(PYTHON) $(COMPARE_SCRIPT) output/output.txt
