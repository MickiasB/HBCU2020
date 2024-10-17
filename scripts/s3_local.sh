#!/bin/bash

# Function to check if MinIO is installed and install it if not
check_minio() {
    if command -v minio &> /dev/null
    then
        echo "MinIO server is already installed."
        minio --version
    else
        echo "MinIO server is not installed. Installing via Homebrew..."
        brew install minio/stable/minio
    fi
}

# Function to check if MinIO Client (mc) is installed and install it if not
check_mc() {
    if command -v mc &> /dev/null
    then
        echo "MinIO Client (mc) is already installed."
        mc --version
    else
        echo "MinIO Client (mc) is not installed. Installing via Homebrew..."
        brew install minio/stable/mc
    fi
}

# Function to run MinIO server
run_minio_server() {
    read -p "Enter the path to the directory you want MinIO to use: " directory

    # # Check if the directory exists
    # if [ ! -d "$directory" ]; then
    #     echo "The directory does not exist. Please create it or specify an existing directory."
    #     exit 1
    # fi

    read -p "Enter your MinIO Access Key: " access_key
    read -sp "Enter your MinIO Secret Key: " secret_key
    echo

    # Set environment variables for Access Key and Secret Key
    export MINIO_ROOT_USER="$access_key"
    export MINIO_ROOT_PASSWORD="$secret_key"

    echo "Starting MinIO server at $directory..."
    minio server "$directory"
}

# Execute the functions
check_minio
check_mc
run_minio_server
