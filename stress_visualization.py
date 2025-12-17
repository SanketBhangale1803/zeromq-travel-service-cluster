import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Configuration
CSV_FILENAME = "experiment_results.csv"
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

def load_data():
    try:
        df = pd.read_csv(CSV_FILENAME)
        # Ensure numeric types
        df['Latency_ms'] = pd.to_numeric(df['Latency_ms'], errors='coerce')
        df['Timestamp'] = pd.to_numeric(df['Timestamp'], errors='coerce')
        df['Concurrency'] = pd.to_numeric(df['Concurrency'], errors='coerce')
        return df
    except FileNotFoundError:
        print(f"Error: {CSV_FILENAME} not found. Run 'complete_system_test.py' first.")
        return None

def plot_scalability(df):
    """
    Generates two plots:
    1. Latency distribution per concurrency level.
    2. System throughput per concurrency level.
    """
    # Filter for Scalability experiment
    data = df[df['Experiment'] == 'Scalability'].copy()
    
    if data.empty:
        print("No Scalability data found in CSV.")
        return

    # Create a figure with two subplots side-by-side
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # --- Plot 1: Latency vs Concurrency (Box Plot) ---
    sns.boxplot(
        data=data, 
        x='Concurrency', 
        y='Latency_ms', 
        hue='Concurrency', 
        palette="viridis", 
        legend=False,
        ax=axes[0]
    )
    axes[0].set_title('Latency Distribution vs. Client Load')
    axes[0].set_xlabel('Number of Concurrent Clients')
    axes[0].set_ylabel('Latency (ms)')
    
    # --- Plot 2: Throughput Calculation & Plot ---
    # Throughput = Number of successful requests / (Max Time - Min Time)
    throughput_data = []
    
    for level in sorted(data['Concurrency'].unique()):
        subset = data[data['Concurrency'] == level]
        successes = subset[subset['Status'] == 'Success']
        
        if len(subset) > 1:
            duration = subset['Timestamp'].max() - subset['Timestamp'].min()
            # Avoid division by zero
            if duration == 0: duration = 0.001
            throughput = len(successes) / duration
        else:
            throughput = 0
            
        throughput_data.append({'Concurrency': level, 'Throughput_RPS': throughput})
    
    tp_df = pd.DataFrame(throughput_data)
    
    sns.lineplot(
        data=tp_df, 
        x='Concurrency', 
        y='Throughput_RPS', 
        marker='o', 
        linewidth=2.5,
        color='coral',
        ax=axes[1]
    )
    # Fill under the line for aesthetics
    axes[1].fill_between(tp_df['Concurrency'], tp_df['Throughput_RPS'], color='coral', alpha=0.1)
    
    axes[1].set_title('System Throughput vs. Client Load')
    axes[1].set_xlabel('Number of Concurrent Clients')
    axes[1].set_ylabel('Throughput (Requests/Sec)')
    
    plt.tight_layout()
    plt.show()

def plot_fault_tolerance(df):
    """
    Generates a timeline scatter plot showing successful bookings 
    and failures during the outage window.
    """
    # Filter for FaultTolerance experiment
    data = df[df['Experiment'] == 'FaultTolerance'].copy()
    
    if data.empty:
        print("No FaultTolerance data found in CSV.")
        return

    # Normalize time to start at 0
    start_time = data['Timestamp'].min()
    data['Relative_Time'] = data['Timestamp'] - start_time

    plt.figure(figsize=(14, 6))

    # Define colors: Green for Success, Red for Failed
    palette = {"Success": "green", "Failed": "red"}

    # Scatter plot
    sns.scatterplot(
        data=data,
        x='Relative_Time',
        y='Latency_ms',
        hue='Status',
        style='Status',
        palette=palette,
        s=100, # Marker size
        alpha=0.8
    )

    # Highlight the outage area roughly
    failures = data[data['Status'] == 'Failed']
    if not failures.empty:
        outage_start = failures['Relative_Time'].min()
        outage_end = failures['Relative_Time'].max()
        
        # Add a shaded region for the downtime
        plt.axvspan(outage_start, outage_end, color='red', alpha=0.1, label='Downtime Window')
        
        # Annotate recovery time
        recovery_duration = outage_end - outage_start
        plt.text(
            outage_start, 
            data['Latency_ms'].max(), 
            f" Recovery: {recovery_duration:.2f}s", 
            color='red', 
            fontweight='bold', 
            verticalalignment='top'
        )

    plt.title('Fault Tolerance Timeline: Leader Failure & Recovery')
    plt.xlabel('Time (seconds from start)')
    plt.ylabel('Request Latency (ms)')
    plt.legend(title='Request Status')
    
    plt.tight_layout()
    plt.show()

def main():
    print("Loading data from CSV...")
    df = load_data()
    
    if df is not None:
        print("\n--- Visualizing Scalability Results ---")
        plot_scalability(df)
        
        print("\n--- Visualizing Fault Tolerance Results ---")
        plot_fault_tolerance(df)

if __name__ == "__main__":
    main()