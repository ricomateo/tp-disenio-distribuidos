.PHONY: all build up down logs clean client test

REPLICAS ?= 3
COMPOSE_NORMAL = -f docker-compose.yaml
COMPOSE_TEST = -f docker-compose-test.yaml
PYTHON = python3
GENERATOR_SCRIPT = generador-compose.py

all: build up

validate-replicas:
	@if [ $(REPLICAS) -lt 1 ]; then \
		echo "Error: REPLICAS must be at least 1"; \
		exit 1; \
	fi

ensure-line-endings:
	@which dos2unix > /dev/null && dos2unix $(GENERATOR_SCRIPT) || true
	@sed -i 's/\r$$//' $(GENERATOR_SCRIPT) || true

generate-compose: ensure-line-endings
	@echo "Generating docker-compose.yaml with $(REPLICAS) join nodes..."
	$(PYTHON) ./$(GENERATOR_SCRIPT) docker-compose.yaml docker-compose.yaml $(REPLICAS)

build:
	@echo "Building Docker images..."
	docker-compose $(COMPOSE_NORMAL) build

up: validate-replicas generate-compose
	docker-compose $(COMPOSE_NORMAL) up -d --build \
		--scale parser=$(REPLICAS) \
        --scale filter_argentina_2000=$(REPLICAS) \
        --scale filter_spain_2000s=$(REPLICAS) \
		--scale sentiment=$(REPLICAS)

test: validate-replicas
	docker-compose $(COMPOSE_TEST) up -d --build \
		--scale parser=$(REPLICAS) \
        --scale test_unique_country=$(REPLICAS) 

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
