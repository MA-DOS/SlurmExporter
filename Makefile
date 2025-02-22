# Variables
APP_NAME = slurm_exporter
SRC_DIR = .
BUILD_DIR = ./bin
GO_FILES = $(wildcard $(SRC_DIR)/*.go)

# Default target
all: build

# Build the application
build:
	mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(APP_NAME) $(GO_FILES)

# Run the application
run: build
	$(BUILD_DIR)/$(APP_NAME)

# Clean the build directory
clean:
	rm -rf $(BUILD_DIR)

# Install dependencies
deps:
	go mod tidy

# Format the code
fmt:
	go fmt $(GO_FILES)

# Lint the code
lint:
	golangci-lint run

.PHONY: all build run clean deps fmt lint