#!/bin/bash

# Check if Redis server is running
redis-cli ping > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Error: Redis server is not running. Please start Redis first."
    echo "You can install Redis with: brew install redis"
    echo "And start it with: brew services start redis"
    exit 1
fi

# Check if hiredis is installed (macOS with Homebrew)
if brew list --formula | grep -q hiredis; then
    HIREDIS_PREFIX=$(brew --prefix hiredis)
elif [ -f /usr/local/include/hiredis/hiredis.h ]; then
    HIREDIS_PREFIX=/usr/local
else
    echo "Error: hiredis development library not found."
    echo "Please install with: brew install hiredis"
    exit 1
fi

echo "Found hiredis at: $HIREDIS_PREFIX"

# Compile the program
echo "Compiling Redis resampler..."
make || { echo "Compilation failed"; exit 1; }

# Run the resampler
echo "Starting Redis resampler..."
./redis_resampler $@

# If we get here, the resampler exited normally
echo "Redis resampler has exited."
