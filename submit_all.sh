#!/bin/bash

# Define the directories in order
DIRS=("seq" "omp" "ff" "mpi")

# Loop through each directory
for i in "${!DIRS[@]}"; do
    DIR="${DIRS[$i]}"
    
    echo "--------------------------------------"
    echo "Processing directory: $DIR"
    
    # Enter directory (exit if it fails)
    cd "$DIR" || { echo "Error: Could not enter $DIR"; exit 1; }
    
    # Submit the job
    sbatch ./new.sh
    
    # Return to parent directory
    cd ..
    
    # Sleep 30 seconds ONLY if this is NOT the last directory
    if [ "$i" -lt "$((${#DIRS[@]} - 1))" ]; then
        echo "Waiting 30 seconds before next submission..."
        sleep 30
    fi
done

echo "--------------------------------------"
echo "All jobs submitted successfully."