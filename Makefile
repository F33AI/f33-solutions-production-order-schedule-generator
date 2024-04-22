.PHONY: tests help version
default: help

local: # Run the web server locally
	@echo "Starting the server locally ..."
	@streamlit run app.py --server.port=8080 --server.address=0.0.0.0

build-and-push-manually: # Push the solver manually
	@echo "Buliding new image..."
	@cd solver && docker build --platform linux/amd64 -t us-central1-docker.pkg.dev/llms-sandbox/f33-solutions/scheduler:latest .
	@echo "Pushing the image..."
	@docker push us-central1-docker.pkg.dev/llms-sandbox/f33-solutions/scheduler:latest

help: # Show help for each of the Makefile recipes.
	@grep -E '^[a-zA-Z0-9 -]+:.*#'  Makefile | sort | while read -r l; do printf "\033[1;32m$$(echo $$l | cut -f 1 -d':')\033[00m:$$(echo $$l | cut -f 2- -d'#')\n"; done