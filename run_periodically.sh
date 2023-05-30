#!/bin/bash

# Command to run periodically (default command)
default_command="sysctl vm.swapusage"

# Interval between each run (in seconds)
interval=0.5

# Output directory
output_directory="./logs/"

# Check if command argument is provided
if [[ $# -eq 0 ]]; then
    command_to_run=$default_command
else
    command_to_run=$@
fi

# Generate the output file name using the current timestamp
timestamp=$(date -u -Iseconds)

while true; do
    output_file="${output_directory}/periodic_run_${timestamp}.txt"
    run_timestamp=$(perl -MTime::Piece -MTime::HiRes=time -E 'say gmtime->datetime . sprintf(".") . sprintf("%06dZ", (split /\./, time())[1])')
    # Run the command and append the result to the output file
    echo "$run_timestamp: $(eval $command_to_run)" >> "$output_file"
    echo "$run_timestamp: $(eval sysctl hw.memsize)" >> "$output_file"
    echo "$run_timestamp: $(eval vm_stat -c 1 1)" >> "$output_file"

    # Sleep for the specified interval
    sleep $interval
done

