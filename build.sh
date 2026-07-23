#!/bin/bash

# Build script for BigData SQL CLI native executable

set -e

echo "🔨 Building BigData SQL CLI..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
BUILD_TYPE="release"
OUTPUT_DIR="build"
PLATFORMS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    --debug)
      BUILD_TYPE="debug"
      shift
      ;;
    --output|-o)
      OUTPUT_DIR="$2"
      shift
      shift
      ;;
    --platform|-p)
      PLATFORMS+=("$2")
      shift
      shift
      ;;
    --all-platforms)
      PLATFORMS=("macos-x64" "macos-arm64" "linux-x64" "linux-arm64" "windows-x64")
      shift
      ;;
    --help|-h)
      echo "Usage: ./build.sh [options]"
      echo ""
      echo "Options:"
      echo "  --debug              Build in debug mode (default: release)"
      echo "  --output DIR         Output directory (default: build)"
      echo "  --platform PLATFORM  Target platform (can be used multiple times)"
      echo "                       Options: macos-x64, macos-arm64, linux-x64, linux-arm64, windows-x64"
      echo "  --all-platforms      Build for all supported platforms"
      echo "  --help               Show this help message"
      echo ""
      echo "Examples:"
      echo "  ./build.sh                           # Build for current platform"
      echo "  ./build.sh --platform linux-x64     # Build for Linux x64"
      echo "  ./build.sh --all-platforms          # Build for all platforms"
      exit 0
      ;;
    *)
      echo -e "${RED}Unknown option: $1${NC}"
      exit 1
      ;;
  esac
done

# If no platforms specified, detect current platform
if [ ${#PLATFORMS[@]} -eq 0 ]; then
  OS=$(uname -s | tr '[:upper:]' '[:lower:]')
  ARCH=$(uname -m)
  
  case "$OS" in
    darwin)
      OS="macos"
      ;;
    linux)
      OS="linux"
      ;;
    mingw*|msys*|cygwin*)
      OS="windows"
      ;;
  esac
  
  case "$ARCH" in
    x86_64|amd64)
      ARCH="x64"
      ;;
    arm64|aarch64)
      ARCH="arm64"
      ;;
  esac
  
  PLATFORMS=("$OS-$ARCH")
  echo -e "${YELLOW}Detected platform: $OS-$ARCH${NC}"
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Get dependencies
echo "📦 Getting dependencies..."
dart pub get

# Build for each platform
for PLATFORM in "${PLATFORMS[@]}"; do
  echo ""
  echo -e "${GREEN}Building for $PLATFORM...${NC}"
  
  # Parse platform
  IFS='-' read -r OS ARCH <<< "$PLATFORM"
  
  # Set output file name
  OUTPUT_FILE="$OUTPUT_DIR/sql-$PLATFORM"
  if [ "$OS" = "windows" ]; then
    OUTPUT_FILE="${OUTPUT_FILE}.exe"
  fi
  
  # Build options
  BUILD_OPTS=""
  
  if [ "$BUILD_TYPE" = "debug" ]; then
    BUILD_OPTS="--enable-asserts"
  fi
  
  # Add platform-specific options
  case "$PLATFORM" in
    macos-x64)
      BUILD_OPTS="$BUILD_OPTS --target-os macos"
      ;;
    macos-arm64)
      BUILD_OPTS="$BUILD_OPTS --target-os macos"
      ;;
    linux-x64)
      BUILD_OPTS="$BUILD_OPTS --target-os linux"
      ;;
    linux-arm64)
      BUILD_OPTS="$BUILD_OPTS --target-os linux"
      ;;
    windows-x64)
      BUILD_OPTS="$BUILD_OPTS --target-os windows"
      ;;
    *)
      echo -e "${RED}Unsupported platform: $PLATFORM${NC}"
      continue
      ;;
  esac
  
  # Compile to native
  echo "Compiling bin/sql.dart to $OUTPUT_FILE..."
  
  if dart compile exe bin/sql.dart -o "$OUTPUT_FILE" $BUILD_OPTS; then
    echo -e "${GREEN}✓ Successfully built $OUTPUT_FILE${NC}"
    
    # Get file size
    if [ -f "$OUTPUT_FILE" ]; then
      SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
      echo "  Size: $SIZE"
    fi
  else
    echo -e "${RED}✗ Failed to build for $PLATFORM${NC}"
  fi
done

echo ""
echo -e "${GREEN}🎉 Build complete!${NC}"
echo ""
echo "To run the SQL CLI:"
echo "  $OUTPUT_DIR/sql-$PLATFORMS --help"
echo ""
echo "To install system-wide:"
echo "  sudo cp $OUTPUT_DIR/sql-$PLATFORMS /usr/local/bin/sql"