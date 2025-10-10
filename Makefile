UV := uv
NPM := npm

BACKEND_DIR := backend
FRONTEND_DIR := frontend
MCP_DIR := mcp_server
POPULATE_DIR := populate_db
UTILS_DIR := utils_package

help:
	@echo "Makefile commands:"
	@echo "  run-local-backend            - Prepare and run the backend locally"
	@echo "  prepare-local-backend        - Prepare the backend environment"
	@echo "  format-backend               - Format the backend code"
	@echo "  run-local-frontend           - Prepare and run the frontend locally"
	@echo "  prepare-local-frontend       - Prepare the frontend environment"
	@echo "  run-local-mcp                - Prepare and run the MCP server locally"
	@echo "  prepare-local-mcp            - Prepare the MCP server environment"
	@echo "  format-mcp                   - Format the MCP server code"
	@echo "  run-local-populate-db        - Prepare and run the populate DB script locally"
	@echo "  prepare-local-populate-db    - Prepare the populate DB environment"
	@echo "  format-populate-db           - Format the populate DB code"
	@echo "  prepare-local-all            - Prepare all local environments"
	@echo "  format-all                   - Format all codebases"
	@echo "  run                          - Start all services using Docker Compose"
	@echo "  stop                         - Stop all services using Docker Compose"
	@echo "  logs                         - View logs of all services"
	@echo "  init-qdrant-data             - Initialize Qdrant with sample data"
	@echo "  init-postgres                - Initialize PostgreSQL database"

.PHONY: \
	run-local-backend prepare-local-backend format-backend \
	run-local-frontend prepare-local-frontend \
	run-local-mcp prepare-local-mcp format-mcp \
	run-local-populate-db prepare-local-populate-db format-populate-db \
	prepare-local-all format-all \
	run stop logs init-qdrant-data init-postgres

prepare-local-backend:
	cd $(BACKEND_DIR) && \
		$(UV) sync && \
		$(UV) pip install -e ../$(UTILS_DIR) && \
		$(UV) pip install -e .

run-local-backend: prepare-local-backend
	cd $(BACKEND_DIR) && $(UV) run src/app/main.py

format-backend:
	cd $(BACKEND_DIR) && $(UV) run ruff format

prepare-local-frontend:
	cd $(FRONTEND_DIR) && $(NPM) install

run-local-frontend: prepare-local-frontend
	cd $(FRONTEND_DIR) && $(NPM) run dev

prepare-local-mcp:
	cd $(MCP_DIR) && \
		$(UV) sync && \
		$(UV) pip install -e . && \
		$(UV) pip install -e ../$(UTILS_DIR)

run-local-mcp: prepare-local-mcp
	cd $(MCP_DIR) && $(UV) run src/mcp/app.py

format-mcp:
	cd $(MCP_DIR) && $(UV) run ruff format

prepare-local-populate-db:
	cd $(POPULATE_DIR) && \
		$(UV) sync && \
		$(UV) pip install -e ../$(UTILS_DIR)

run-local-populate-db: prepare-local-populate-db
	cd $(POPULATE_DIR) && $(UV) run src/populate_db.py

format-populate-db:
	cd $(POPULATE_DIR) && $(UV) run ruff format

prepare-local-all: prepare-local-backend prepare-local-frontend prepare-local-mcp prepare-local-populate-db

format-all: format-backend format-mcp format-populate-db

run:
	docker compose up -d

stop:
	docker compose down

logs:
	docker compose logs -f

init-qdrant-data: prepare-local-populate-db
	cd $(POPULATE_DIR) && $(UV) run src/populate_db.py

init-postgres: prepare-local-backend
	cd $(BACKEND_DIR) && $(UV) run src/app/db/init_db.py
