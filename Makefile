# Makefile for BigData SQL CLI

.PHONY: help build build-debug install uninstall clean test run-example

# Default target
help:
	@echo "BigData SQL CLI - Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  make build          - Build release executable for current platform"
	@echo "  make build-debug    - Build debug executable"
	@echo "  make build-all      - Build for all supported platforms"
	@echo "  make install        - Build and install to /usr/local/bin"
	@echo "  make uninstall      - Remove from /usr/local/bin"
	@echo "  make clean          - Clean build artifacts"
	@echo "  make test           - Run tests"
	@echo "  make run-example    - Run example queries"
	@echo ""

# Build release executable
build:
	@echo "Building release executable..."
	@./build.sh

# Build debug executable
build-debug:
	@echo "Building debug executable..."
	@./build.sh --debug

# Build for all platforms
build-all:
	@echo "Building for all platforms..."
	@./build.sh --all-platforms

# Install to system
install: build
	@echo "Installing to /usr/local/bin..."
	@OS=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
	ARCH=$$(uname -m); \
	case "$$OS" in \
		darwin) OS="macos" ;; \
		linux) OS="linux" ;; \
	esac; \
	case "$$ARCH" in \
		x86_64|amd64) ARCH="x64" ;; \
		arm64|aarch64) ARCH="arm64" ;; \
	esac; \
	SOURCE="build/sql-$$OS-$$ARCH"; \
	if [ "$$OS" = "windows" ]; then SOURCE="$$SOURCE.exe"; fi; \
	if [ -f "$$SOURCE" ]; then \
		sudo cp "$$SOURCE" /usr/local/bin/sql && \
		sudo chmod +x /usr/local/bin/sql && \
		echo "✓ Installed sql to /usr/local/bin"; \
	else \
		echo "✗ Build file not found: $$SOURCE"; \
		exit 1; \
	fi

# Uninstall from system
uninstall:
	@echo "Uninstalling from /usr/local/bin..."
	@sudo rm -f /usr/local/bin/sql
	@echo "✓ Removed sql from /usr/local/bin"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf build/
	@rm -f *.aot
	@rm -f *.snapshot
	@echo "✓ Clean complete"

# Run tests
test:
	@echo "Running tests..."
	@dart test

# Run example queries
run-example: build
	@echo "Running example queries..."
	@echo ""
	@echo "Creating sample data..."
	@echo "id,name,age,salary,department" > /tmp/employees.csv
	@echo "1,Alice,30,70000,Engineering" >> /tmp/employees.csv
	@echo "2,Bob,35,80000,Sales" >> /tmp/employees.csv
	@echo "3,Charlie,28,60000,Engineering" >> /tmp/employees.csv
	@echo "4,Diana,32,75000,Marketing" >> /tmp/employees.csv
	@echo "5,Eve,29,65000,HR" >> /tmp/employees.csv
	@echo "Sample data created at /tmp/employees.csv"
	@echo ""
	@OS=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
	ARCH=$$(uname -m); \
	case "$$OS" in \
		darwin) OS="macos" ;; \
		linux) OS="linux" ;; \
	esac; \
	case "$$ARCH" in \
		x86_64|amd64) ARCH="x64" ;; \
		arm64|aarch64) ARCH="arm64" ;; \
	esac; \
	SQL="build/sql-$$OS-$$ARCH"; \
	if [ "$$OS" = "windows" ]; then SQL="$$SQL.exe"; fi; \
	echo "1. Count employees:"; \
	$$SQL -c employees:/tmp/employees.csv -e "SELECT COUNT(*) as total FROM employees"; \
	echo ""; \
	echo "2. Average salary by department:"; \
	$$SQL -c employees:/tmp/employees.csv -e "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department"; \
	echo ""; \
	echo "3. Employees over 30:"; \
	$$SQL -c employees:/tmp/employees.csv -e "SELECT name, age FROM employees WHERE age > 30 ORDER BY age DESC"

# Development helpers
dev-run:
	@dart run bin/sql.dart

dev-watch:
	@echo "Watching for changes..."
	@while true; do \
		find lib bin -name "*.dart" | entr -d make build; \
	done