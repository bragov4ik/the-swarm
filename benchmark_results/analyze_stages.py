# NOTE: CODE WRITTEN BY CHATGPT
import csv
import statistics
import sys


def parse_csv(filename):
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        data = {field: []
                for field in reader.fieldnames if field != 'Timestamp'}

        for row in reader:
            for field in data.keys():
                data[field].append(float(row[field]))

    return data


def calculate_stats(data):
    stats = {}

    for field, values in data.items():
        mean = statistics.mean(values)
        stdev = statistics.stdev(values)
        stats[field] = {'mean': mean, 'std_dev': stdev}

    return stats


def print_stats(stats):
    for field, values in stats.items():
        print(f"({field}, {values['mean']}) +- (0, {values['std_dev']})")


if len(sys.argv) < 2:
    print("Please provide the CSV filename as an argument.")
    sys.exit(1)

filename = sys.argv[1]

data = parse_csv(filename)
stats = calculate_stats(data)
print_stats(stats)
