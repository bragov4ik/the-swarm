from collections import defaultdict
import os
import sys
from datetime import datetime, timedelta
import tqdm

from attr import dataclass


def round_up_timestamp(timestamp, rounding_duration):
    # Parse the timestamp string into a datetime object
    dt = datetime.fromisoformat(timestamp)

    # Calculate the rounding duration as timedelta
    duration = timedelta(**rounding_duration)

    # Calculate the remainder to determine the rounding direction
    remainder = dt.timestamp() % duration.total_seconds()

    # Perform rounding by adding or subtracting the remainder
    if remainder >= duration.total_seconds() / 2:
        rounded_dt = dt + (duration - timedelta(seconds=remainder))
    else:
        rounded_dt = dt - timedelta(seconds=remainder)

    rounded_dt = rounded_dt.replace(microsecond=0)

    # Convert the rounded datetime back to an ISO 8601 formatted string
    rounded_timestamp = rounded_dt.isoformat()

    return rounded_timestamp


round_to = {
    'minutes': 0,
    'seconds': 1
}

times = []


# @dataclass
# class LogEntry:
#     timestamp: str
#     log_level: str
#     target: str
#     message: str


def analyze_log_entry(timestamp, log_level, target, message):
    aggregated_timestamp = round_up_timestamp(timestamp, round_to)
    if len(times) == 0 or times[-1][0] != aggregated_timestamp:
        times.append([aggregated_timestamp, 1])
    else:
        # timestamp matches
        times[-1][1] += 1


def update_progress(progress):
    size_str = str(progress) + ' lines handled!'
    sys.stdout.write('%s\r' % size_str)
    sys.stdout.flush()


def analyze_large_log_file(file_path):
    with open(file_path, "r") as file:
        with tqdm.tqdm(total=os.path.getsize(file_path)) as pbar:
            for line in file:
                timestamp, log_level, tail = line.split(" ", 2)
                target, message = tail.split(":", 1)
                analyze_log_entry(timestamp, log_level, target, message)

                pbar.update(len(line))


# def sort_timestamps(timestamps):
#     # Sort the dictionary by timestamp values and convert to a list of tuples
#     sorted_timestamps = sorted(
#         timestamps.items(), key=lambda x: datetime.fromisoformat(x[0]))

#     # Create a list of pairs (timestamp, length) with lengths of the stored lists
#     length_pairs = [(timestamp, len(data))
#                     for timestamp, data in sorted_timestamps]

#     # Sort the list of pairs based on timestamp values
#     sorted_length_pairs = sorted(
#         length_pairs, key=lambda x: datetime.fromisoformat(x[0]))

#     return sorted_length_pairs


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <file_path>")
        sys.exit(1)

    log_file_path = sys.argv[1]
    analyze_large_log_file(log_file_path)
    [print(i) for i in times]
