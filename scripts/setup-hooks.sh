#!/bin/bash
# Setup Git hooks for the project

# Print colorized output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Setting up Git hooks...${NC}"

# Check if pre-commit is installed
if ! command -v pre-commit &> /dev/null; then
    echo -e "${RED}Error: pre-commit is not installed.${NC}"
    echo -e "Please install it first:"
    echo -e "  ${YELLOW}brew install pre-commit${NC} (on macOS with Homebrew)"
    echo -e "  ${YELLOW}pip install pre-commit${NC} (with Python's pip)"
    exit 1
fi

# Install the pre-commit hook
pre-commit install

# Run pre-commit on all files
echo -e "${YELLOW}Running initial pre-commit on all files...${NC}"
pre-commit run --all-files

echo -e "${GREEN}Git hooks setup complete!${NC}"
echo -e "${YELLOW}These hooks will now run automatically before each commit.${NC}"
