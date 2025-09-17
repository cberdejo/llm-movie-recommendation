prepare:
	uv sync
	uv pip install -e .
	uv pip install -e ../utils_package
run: 
	uv run src/mcp/app.py

format: 
	uv run ruff format