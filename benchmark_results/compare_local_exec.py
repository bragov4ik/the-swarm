# NOTE: CODE WRITTEN BY CHATGPT
import pandas as pd


def calculate_stats(data):
    stats = data.describe().transpose()
    return stats


def print_stats(stats):
    fields = stats.index.tolist()
    num_fields = len(fields)

    print("[")
    print("  ytick={" + ",".join(str(i+1) for i in range(num_fields)) + "},")
    print("  yticklabels={" + ", ".join(fields) + "},")
    print("]")

    for index, row in stats.iterrows():
        median = row['50%']
        upper_quartile = row['75%']
        lower_quartile = row['25%']
        upper_whisker = row['max']
        lower_whisker = row['min']

        print("\\addplot+[")
        print("  boxplot prepared={")
        print(f"    median={median},")
        print(f"    upper quartile={upper_quartile},")
        print(f"    lower quartile={lower_quartile},")
        print(f"    upper whisker={upper_whisker},")
        print(f"    lower whisker={lower_whisker}")
        print("  },")
        print("] coordinates {};")
        print()


# Import the first CSV file and extract the "ExecutionTime" column
data1 = pd.read_csv("./benchmark_results/local_pc/local.csv")
execution_time = data1["ExecutionTime"]

# Import the second CSV file and extract the "LocalExecution" column
data2 = pd.read_csv("./benchmark_results/true_distributed_pc/distributed.csv")
local_execution = data2["LocalExecution"]

# Create a new DataFrame with the two columns
combined_data = pd.DataFrame({
    "ExecutionTime": execution_time,
    "LocalExecution": local_execution
})

# Calculate statistics for each column
stats = calculate_stats(combined_data)
print_stats(stats)
