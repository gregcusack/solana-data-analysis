import pandas as pd
import json
import matplotlib.pyplot as plt
import sys
import mplcursors
import numpy as np
import os
from TopLevel import *
from TransformData import *

stake_columns_to_drop = [
    "lastVote", 
    "rootSlot", 
    "credits", 
    "epochCredits", 
    "delinquent", 
    "skipRate", 
    "commission"
]

version_columns_to_drop = [
    "time",
]

aggregator_types = [
    "mean",
    "median"
]

MINIMUM_VALIDATOR_VERSION = (1, 16, 2)

def load_data(data_type):
    directory = "../data_2/"
    files = [directory + filename for filename in os.listdir(directory) if filename.startswith(data_type)]
    print(files)
    df = pd.concat(map(pd.read_csv, files))
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    df = df \
            .sort_values(by=["time", "host_id"]) \
            .reset_index(drop=True) \
            .drop_duplicates(subset=["time", "host_id"], keep="first")
    return df

def aggregate_data_frame_by(df, data_type: str, aggregator: str):
    column_name = "mean_" + data_type
    if aggregator == "median":
        aggregated_df = df.groupby('time')[column_name].median().reset_index()
    else:
        aggregated_df = df.groupby('time')[column_name].mean().reset_index()
    return aggregated_df


def get_validator_data(data_type):
    # file_name = "../data/"+ data_type + "_7_2_4am_7_6_4am_1hr.csv"
    # file_name = "../data/"+ data_type + "_last_30_days_until_7_10_4pm_1hr.csv"

    if data_type == "packets_sent_push_messages_count":
        file_name_1 = "../data/"+ data_type + "_last_30_days_until_7_10_4pm_1hr.csv"
        file_name_2 = "../data/"+ data_type + "_last_30_days_until_7_18_6pm_1hr.csv"
        file_name_3 = "../data/last_30_days_" + data_type + "_7_6_7_21_1hr.csv" 
        df_1 = pd.read_csv(file_name_1)
        df_2 = pd.read_csv(file_name_2)
        df_3 = pd.read_csv(file_name_3)
        df_1["time"] = pd.to_datetime(df_1["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        df_2["time"] = pd.to_datetime(df_2["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        df_3["time"] = pd.to_datetime(df_3["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        combined_df = pd.concat([df_1, df_2, df_3])
        # Sort the dataframe by "time" and "host_id"
        combined_df = combined_df.sort_values(by=["time", "host_id"])

        # Reset the index to reindex the dataframe
        combined_df = combined_df.reset_index(drop=True)

        # Drop duplicates based on "time" and "host_id"
        combined_df = combined_df.drop_duplicates(subset=["time", "host_id"], keep="first")
        return combined_df
    elif "validator" in data_type:
        data_type = "validator_new"
        file_name_1 = "../data/last_30_days_" + data_type + "_7_2_7_16_1hr.csv" 
        file_name_2 = "../data/last_30_days_" + data_type + "_7_6_7_21_1hr.csv" 
        df_1 = pd.read_csv(file_name_1)
        df_2 = pd.read_csv(file_name_2)
        df_1["time"] = pd.to_datetime(df_1["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        df_2["time"] = pd.to_datetime(df_2["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        combined_df = pd.concat([df_1, df_2])
        # Sort the dataframe by "time" and "host_id"
        combined_df = combined_df.sort_values(by=["time", "host_id"])

        # Reset the index to reindex the dataframe
        combined_df = combined_df.reset_index(drop=True)

        # Drop duplicates based on "time" and "host_id"
        combined_df = combined_df.drop_duplicates(subset=["time", "host_id"], keep="first")
        filtered_df = combined_df[combined_df['host_id'] == 'AgcvBSS97jBoKY2x1LXrqScziFx1jpCzdE2UpgSiVeQr']
        print("###############################")
        print(filtered_df["host_id"], filtered_df["time"])
        return combined_df
    elif data_type == "gossip_listen_loop_time":
        file_name_1 = "../data/last_30_days_" + data_type + "_7_2_7_16_1hr.csv" 
        file_name_2 = "../data/last_30_days_" + data_type + "_7_10_7_25_1hr.csv" 
        df_1 = pd.read_csv(file_name_1)
        df_2 = pd.read_csv(file_name_2)
        df_1["time"] = pd.to_datetime(df_1["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        df_2["time"] = pd.to_datetime(df_2["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        combined_df = pd.concat([df_1, df_2])
        # Sort the dataframe by "time" and "host_id"
        combined_df = combined_df.sort_values(by=["time", "host_id"])

        # Reset the index to reindex the dataframe
        combined_df = combined_df.reset_index(drop=True)

        # Drop duplicates based on "time" and "host_id"
        combined_df = combined_df.drop_duplicates(subset=["time", "host_id"], keep="first")
        return combined_df
    elif data_type == "new_pull_requests_count" or data_type == "table_size":
        file_name = "../data/last_30_days_" + data_type + "_7_10_7_25_1hr.csv" 
    else:
        file_name = "../data/last_30_days_" + data_type + "_7_2_7_16_1hr.csv" 
        # file_name = "../data/last_30_days_" + data_type + "_7_6_7_21_1hr.csv" 
        # file_name = "../data/" + data_type + "_last_30_days_until_7_10_4pm_1hr.csv" 


    # df = pd.read_csv ('../data/" + data_type + "packets_sent_gossip_requests_count_7_2_4am-7_4_4am_1hr.csv')
    df = pd.read_csv(file_name)
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    return df

def get_validator_stakes():
    data = json.load(open('../data/validator_stakes_new.json'))
    stakes = pd.DataFrame(data["validators"])
    stakes = stakes.drop(stake_columns_to_drop, axis=1)
    stakes['version_tuple'] = stakes['version'].apply(lambda x: tuple(map(int, x.split('.'))) if x != "unknown" else None)

    # Define the target version
    target_version = (1, 16, 2)

    # Filter out rows with a version number less than the target version
    stakes_filtered = stakes[stakes['version_tuple'] >= target_version]

    # Drop the temporary version_tuple column if you don't need it anymore
    stakes_filtered = stakes_filtered.drop(columns=['version_tuple'])

    stakes_filtered = stakes_filtered.sort_values(by='activatedStake')
    stakes_filtered['activatedStake'] = stakes_filtered['activatedStake'].div(1000000000).round(9)
    stakes_filtered.rename(columns = {'identityPubkey':'host_id'}, inplace = True)

    return stakes_filtered

def merge_dataframes(df1, df2):
    all_data = pd.merge(df1, df2, on="host_id", how="inner")

    return all_data


def get_dataframe_percentile(df, bottom_percentile, top_percentile):
    # Calculate the threshold for the top 10% of "activatedStake"

    df['percent_rank'] = df['activatedStake'].rank(pct=True)

    df_filtered = df[(df['percent_rank'] >= bottom_percentile / 100) & (df['percent_rank'] < top_percentile / 100)]
    print(df_filtered)

    unique_host_ids = df_filtered['host_id'].unique()
    print("Unique Host IDs in percentile: " + str(len(unique_host_ids)))

    return df_filtered

def plot_dataframe(df, plot_title, data_type, set_y_axis_limits):
    # Group the data by "host_id" and plot each group
    data_name = "mean_" + data_type
    plt.figure(figsize=(18, 10))  # Width: 12 inches, Height: 6 inches
    lines = []
    groupby_obj = df.groupby("host_id")
    for host_id, group in groupby_obj:
        label = f"{host_id} ({group['activatedStake'].iloc[0]})"
        line, = plt.plot(group["time"], group[data_name], label=label)
        lines.append(line)

    line_time = pd.to_datetime("2023-07-03 04:10:00")
    plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')

    # Set the x-axis label
    plt.xlabel("time")

    # Set the y-axis label
    plt.ylabel(data_name)
    
    # Place the legend below the graph
    plt.legend(bbox_to_anchor=(0.5, -0.05), loc="upper center", ncol=3, frameon=False, fontsize='small')

    if set_y_axis_limits:
        y_min = 9000
        y_max = 23000
        # Set the y-axis limits
        plt.ylim(y_min, y_max)

    # Adjust the layout to accommodate the legend
    plt.subplots_adjust(bottom=0.25)
    plt.grid()

    # # Set the chart title
    # plt.title("Packets Sent Push Messages Count. " + str(bottom_percentile) + "-" + str(top_percentile) + "%-ile by stake")
    plt.title(data_name + ": " + plot_title)

    # Create the cursor object
    cursor = mplcursors.cursor(hover=True)

    # Add tooltips to the lines
    for line in lines:
        tooltip_text = line.get_label()

        @cursor.connect("add")
        def on_add(sel):
            # Get the label from the hovered line
            label = sel.annotation.get_text()

            # Update the cursor's annotation with the new label
            sel.annotation.set_text(label)
            
    # Display the plot
    plt.show()

def get_top_N_largest_engress_by_host_id(df, N):
    data_type = "packets_sent_push_messages_count"
    column_name = "mean_" + data_type
    # Filter the DataFrame for the first 5 hours
    first_5_hours_df = df[df['time'] < (df['time'].min() + pd.Timedelta(hours=5))]

    # Calculate the mean of "egress_messages" for the first 5 hours for each "host_id"
    mean_first_5_hours = first_5_hours_df.groupby('host_id')[column_name].mean()

    # Filter the DataFrame for the last 5 hours
    last_5_hours_df = df[df['time'] > (df['time'].max() - pd.Timedelta(hours=5))]

    # Calculate the mean of "egress_messages" for the last 5 hours for each "host_id"
    mean_last_5_hours = last_5_hours_df.groupby('host_id')[column_name].mean()

    # Calculate the difference between the means for each "host_id"
    difference_means = mean_last_5_hours - mean_first_5_hours

    # Sort the "host_id" by the difference in descending order to get the top N host_ids
    top_N_host_ids = difference_means.sort_values(ascending=False).head(N)

    print("top 10 movers:")
    print(top_N_host_ids.head(10))

    return top_N_host_ids

def plot_top_N_largest_increases_in_egress(df, validator_new_data, N, bottom_percentile, top_percentile):
    top_N_host_ids = get_top_N_largest_engress_by_host_id(df, N)
    # top_N_host_ids = get_top_N_validators_by_number_of_restarts(validator_new_data, N)
    data_type = "packets_sent_push_messages_count"
    column_name = "mean_" + data_type
    print(top_N_host_ids)
   
    df['percentile_rank'] = df['activatedStake'].rank(pct=True) * 100
    top_N_df = df[df['host_id'].isin(top_N_host_ids.index)]

    top_N_df = top_N_df.drop_duplicates(subset='host_id')

    for _, row in top_N_df.iterrows():
        host_id = row['host_id']
        mean_diff = top_N_host_ids[host_id]
        activated_stake = row['activatedStake']
        percentile_rank = row['percentile_rank']

        print(f"Host ID: {host_id}, Mean Difference in Egress Messages: {mean_diff:.2f}, Activated Stake: {activated_stake:.2f}, Percentile Rank: {percentile_rank:.2f}%")

    plt.figure(figsize=(18, 10))  # Width: 12 inches, Height: 6 inches

    colors = plt.cm.tab20.colors
    index = 0
    for host_id in top_N_host_ids.index:
        color_idx = index % len(colors)
        host_id_data = df[df['host_id'] == host_id]
        plt.plot(host_id_data['time'], host_id_data[column_name], label=host_id, color=colors[color_idx])
        index += 1
    

    line_time = pd.to_datetime("2023-07-03 04:10:00")
    plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')
    line_time = pd.to_datetime("2023-07-10 21:22:00")
    plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
    line_time = pd.to_datetime("2023-07-17 18:44:00")
    plt.axvline(x=line_time, color='green', linestyle='--', label='1.16.4 Activation Date')
    plt.xlabel('Time')
    plt.ylabel(column_name)
    plot_title = 'Top ' + str(N) + ' Host IDs in ' + str(bottom_percentile) + '-' + str(top_percentile) + '%ile with Largest Increase in ' + column_name
    plt.title(plot_title)
    plt.legend(title='Host ID', bbox_to_anchor=(0.5, -0.1), loc='upper center', ncol=3)
    plt.tight_layout()
    plt.grid(True)
    plt.savefig('./plots/' + plot_title + '.png', dpi=300)
    plt.show()



def run_end_check_filtering(df, percent_diff, data_name):
    filtered_host_ids = []
    # Group the data by host_id
    groupby_obj = df.groupby("host_id")
    
    for host_id, group in groupby_obj:
        # Get the first 5 entries of messages sent
        first_5_entries = group.head(5)[data_name]
        # print(first_5_entries)
        
        # Get the last 5 entries of messages sent
        last_5_entries = group.tail(5)[data_name]
        # print(last_5_entries)


        # Calculate the percentage difference between the first and last 5 entries
        percent_change = (last_5_entries.mean() - first_5_entries.mean()) / first_5_entries.mean() * 100
        # print("percent_change: " + str(percent_change))
        # Check if the percentage difference is within the specified threshold
        if percent_change <= percent_diff:
            filtered_host_ids.append(host_id)
            # print("suhhh")
        
    # Filter the original DataFrame based on the filtered host_ids
    df_filtered = df[~df["host_id"].isin(filtered_host_ids)]

    return df_filtered

def find_large_changes_in_data_between_ends(df, percent_diff, data_type):
    data_name = "mean_" + data_type
    df_filtered = run_end_check_filtering(df, percent_diff, data_name)

    unique_host_ids = df_filtered['host_id'].unique()
    print("Unique Host IDs that increased by " + str(percent_diff) + "%: " + str(len(unique_host_ids)))

    return df_filtered

def print_query(data_type, host_ids):
    host_ids_string = ""
    for host_id in host_ids:
        host_ids_string = host_ids_string + "host_id=\'" + host_id + "\' OR "
    host_ids_string = host_ids_string[:-3]  # rm the last "OR "

    start = "SELECT mean(\"" + data_type + "\") AS \"" + data_type + "\" \
        FROM \"tds\".\"autogen\".\"cluster_info_stats5\" \
        WHERE time > :dashboardTime: AND time < :upperDashboardTime: \
        AND "

    # ending = "GROUP BY time(1h), \"host_id\" FILL(null)"
    ending = "GROUP BY time(1h) FILL(null)"


    query = start + host_ids_string + ending
    print(query)

def get_df_post_activation(df):
    filtered_df = df.copy()

    # Convert the date column to datetime if needed
    filtered_df['time'] = pd.to_datetime(filtered_df['time'])

    # Filter out values after a specific date and time
    specific_date = pd.to_datetime("2023-07-02 16:10:00")
    filtered_df = filtered_df.loc[filtered_df['time'] >= specific_date]

    return filtered_df

def plot_by_time_aggregator(df, data_type, aggregator, bottom_percentile, top_percentile, *args):
    which_N = "all"
    if args:
        which_N = args[0]
        count = args[1]
    column_name = "mean_" + data_type
    data_name = aggregator + "_" + data_type
    if data_type == "push_pull_ratio":
        column_name = "push_pull_ratio"
    if aggregator == "median":
        aggregated_df = df.groupby('time')[column_name].median().reset_index()
    else:
        aggregated_df = df.groupby('time')[column_name].mean().reset_index()

    plt.figure(figsize=(18, 7))  # Width: 12 inches, Height: 6 inches
    plt.grid()
    line_time = pd.to_datetime("2023-07-03 04:10:00")
    plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')
    # if data_type == "packets_sent_push_messages_count":
    line_time = pd.to_datetime("2023-07-10 21:22:00")
    plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
    line_time = pd.to_datetime("2023-07-17 18:44:00")
    plt.axvline(x=line_time, color='green', linestyle='--', label='1.16.4 Activation Date')

    # plt.ylim(30000, 38000)  # Set the maximum limit to 100
    plot_title = data_name + '_' + str(bottom_percentile) + '-' + str(top_percentile) + '% stake. '
    plot_title += which_N
    if which_N != "all":
        plot_title += " " + str(count)
    plot_title += " validators"

    # Plot the mean values
    plt.plot(aggregated_df['time'], aggregated_df[column_name])
    plt.xlabel('Time')
    plt.ylabel(data_name)
    plt.title(plot_title)
    plt.savefig('./plots/' + plot_title + '.png', dpi=300)

    plt.show()

def do_validator_new(data):
    data.set_index("time", inplace=True)
    unique_hosts_count = data.resample('1H')['host_id'].nunique()
    plt.figure(figsize=(18, 7))  # Width: 12 inches, Height: 6 inches
    plt.grid()
    line_time = pd.to_datetime("2023-07-03 04:10:00")
    plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')
    line_time = pd.to_datetime("2023-07-10 21:22:00")
    plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
    line_time = pd.to_datetime("2023-07-17 18:44:00")
    plt.axvline(x=line_time, color='green', linestyle='--', label='1.16.4 Activation Date')

    plot_title = 'validator-new_host_id_counts_by_time'

    plt.plot(unique_hosts_count.index, unique_hosts_count.values, marker='o', linestyle='-', markersize=3)


    # Plot the mean values
    plt.xlabel('Time')
    plt.ylabel("Aggregate Number of Validator Restarts per Hour")
    plt.title(plot_title)
    plt.savefig('./plots/' + plot_title + '.png', dpi=300)
    plt.show()

def do_validator_restarts(data_df, validator_new_df, N):
    top_N_host_ids = get_top_N_largest_engress_by_host_id(data_df, N)

    validator_new_df = validator_new_df[validator_new_df['host_id'].isin(top_N_host_ids.index)]
    validator_new_df["time"] = pd.to_datetime(validator_new_df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    validator_new_df = validator_new_df[['time', 'host_id']]

    hourly_counts = validator_new_df.groupby('host_id').resample('1H', on="time").count()
    hourly_counts = hourly_counts.loc[(hourly_counts != 0).any(axis=1)]
    # Unstack the 'host_id' index to create separate columns for each host_id
    hourly_counts = hourly_counts.unstack('host_id')

    # Set the figure size (adjust as needed)
    plt.figure(figsize=(18, 10))  # Width: 12 inches, Height: 6 inches
    # Create the plot using plt.plot()
    colors = plt.cm.tab20.colors
    index = 0
    for host_id in hourly_counts.columns.levels[1]:
        color_idx = index % len(colors)
        x_values = hourly_counts.index
        # jittered_x_values = x_values + pd.Timedelta(np.random.uniform(-jitter / 2, jitter / 2, len(x_values)), unit='s')
        jitter = pd.to_timedelta(np.random.uniform(-1, 1, size=len(x_values)), unit='h')
        jittered_x_values = x_values + jitter
        plt.scatter(jittered_x_values, hourly_counts[('host_id', host_id)], marker='o', s=100, label=host_id, color=colors[color_idx])
        index += 1

    line_time_1_16_2 = pd.to_datetime("2023-07-03 04:10:00")
    plt.axvline(x=line_time_1_16_2, color='red', linestyle='--', label='1.16.2 Activation Date')
    line_time = pd.to_datetime("2023-07-10 21:22:00")
    plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
    line_time = pd.to_datetime("2023-07-17 18:44:00")
    plt.axvline(x=line_time, color='green', linestyle='--', label='1.16.4 Activation Date')

    time_min = hourly_counts.index.min()
    if time_min > line_time_1_16_2:
        time_min = line_time_1_16_2  - pd.Timedelta(hours=6)
    plt.xlim(time_min, data_df['time'].max())
    plt.ylim(plt.ylim(ymin=0))
    plot_title = 'validator-restarts_for_top_' + str(N) + ' validators'

    # Plot the mean values
    plt.xlabel('Time')
    plt.ylabel("Number of Validator Restarts Per Hour")
    plt.title(plot_title)
    plt.grid(True)
    plt.legend(title='Host ID', bbox_to_anchor=(0.5, -0.1), loc='upper center', ncol=3)
    plt.tight_layout()
    plt.savefig('./plots/' + plot_title + '.png', dpi=300)

    plt.show()

def get_top_N_validators_by_number_of_restarts(validator_df, N):
    validator_df["time"] = pd.to_datetime(validator_df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    validator_new_df = validator_df[['time', 'host_id']]
    validator_new_df.set_index('time', inplace=True)
    # print(validator_new_df.head(20))
    validator_new_df = validator_new_df.groupby('host_id')
    # print(validator_2.head(20))
    validator_restart_counts = validator_new_df.value_counts()

    validator_restart_counts.sort_values(ascending=False, inplace=True)
    print(validator_restart_counts.head(N))
    top_N_host_ids = validator_restart_counts.head(N)
    return top_N_host_ids

def get_top_N_restarted_validators(data_df, validator_df, N):
    top_N_host_ids = get_top_N_validators_by_number_of_restarts(validator_df, N)
    validator_df = validator_df[validator_df['host_id'].isin(top_N_host_ids.index)]

    validator_df["time"] = pd.to_datetime(validator_df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    validator_new_df = validator_df[['time', 'host_id']]

    hourly_counts = validator_new_df.groupby('host_id').resample('1H', on="time").count()
    hourly_counts = hourly_counts.loc[(hourly_counts != 0).any(axis=1)]
    # Unstack the 'host_id' index to create separate columns for each host_id
    hourly_counts = hourly_counts.unstack('host_id')

    # Set the figure size (adjust as needed)
    plt.figure(figsize=(18, 10))  # Width: 12 inches, Height: 6 inches
    # Create the plot using plt.plot()
    colors = plt.cm.tab20.colors
    index = 0
    for host_id in hourly_counts.columns.levels[1]:
        color_idx = index % len(colors)
        x_values = hourly_counts.index
        # jittered_x_values = x_values + pd.Timedelta(np.random.uniform(-jitter / 2, jitter / 2, len(x_values)), unit='s')
        jitter = pd.to_timedelta(np.random.uniform(-1, 1, size=len(x_values)), unit='h')
        jittered_x_values = x_values + jitter
        plt.scatter(jittered_x_values, hourly_counts[('host_id', host_id)], marker='o', s=100, label=host_id, color=colors[color_idx])
        index += 1

    line_time_1_16_2 = pd.to_datetime("2023-07-03 04:10:00")
    plt.axvline(x=line_time_1_16_2, color='red', linestyle='--', label='1.16.2 Activation Date')
    line_time = pd.to_datetime("2023-07-10 21:22:00")
    plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
    line_time = pd.to_datetime("2023-07-17 18:44:00")
    plt.axvline(x=line_time, color='green', linestyle='--', label='1.16.4 Activation Date')

    time_min = hourly_counts.index.min()
    if time_min > line_time_1_16_2:
        time_min = line_time_1_16_2  - pd.Timedelta(hours=6)
    plt.xlim(time_min, data_df['time'].max())
    plt.ylim(plt.ylim(ymin=0))
    plot_title = 'Top ' + str(N) + ' most restarted validators'

    # Plot the mean values
    plt.xlabel('Time')
    plt.ylabel("Number of Validator Restarts Per Hour")
    plt.title(plot_title)
    plt.grid(True)
    plt.legend(title='Host ID', bbox_to_anchor=(0.5, -0.1), loc='upper center', ncol=3)
    plt.tight_layout()
    plt.savefig('./plots/' + plot_title + '.png', dpi=300)

    plt.show()

def get_validator_data_overlap(data_type):
    if data_type == "packets_sent_push_messages_count_abridged":
        data_type = "packets_sent_push_messages_count"
        file_name = "../data/last_30_days_" + data_type + "_7_10_7_25_1hr.csv" 
        df = pd.read_csv(file_name)
        df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        return df, 0
    else:
        data_type = "packets_sent_push_messages_count"
        # file_name_1 = "../data/"+ data_type + "_last_30_days_until_7_10_4pm_1hr.csv"
        file_name_2 = "../data/"+ data_type + "_last_30_days_until_7_18_6pm_1hr.csv"
        file_name_3 = "../data/last_30_days_" + data_type + "_7_6_7_21_1hr.csv" 
        # df_1 = pd.read_csv(file_name_1)
        df_2 = pd.read_csv(file_name_2)
        df_3 = pd.read_csv(file_name_3)
        # df_1["time"] = pd.to_datetime(df_1["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        df_2["time"] = pd.to_datetime(df_2["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        df_3["time"] = pd.to_datetime(df_3["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        combined_df_1 = pd.concat([df_2, df_3])
        # combined_df_1 = pd.concat([df_1, df_2, df_3])
        # Sort the dataframe by "time" and "host_id"
        combined_df_1 = combined_df_1.sort_values(by=["time", "host_id"])

        # Reset the index to reindex the dataframe
        combined_df_1 = combined_df_1.reset_index(drop=True)

        # Drop duplicates based on "time" and "host_id"
        combined_df_1 = combined_df_1.drop_duplicates(subset=["time", "host_id"], keep="first")

    data_type = "validator_new"
    file_name_1 = "../data/last_30_days_" + data_type + "_7_2_7_16_1hr.csv" 
    file_name_2 = "../data/last_30_days_" + data_type + "_7_6_7_21_1hr.csv" 
    df_1 = pd.read_csv(file_name_1)
    df_2 = pd.read_csv(file_name_2)
    df_1["time"] = pd.to_datetime(df_1["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    df_2["time"] = pd.to_datetime(df_2["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    combined_df_2 = pd.concat([df_1, df_2])
    # Sort the dataframe by "time" and "host_id"
    combined_df_2 = combined_df_2.sort_values(by=["time", "host_id"])

    # Reset the index to reindex the dataframe
    combined_df_2 = combined_df_2.reset_index(drop=True)

    # Drop duplicates based on "time" and "host_id"
    combined_df_2 = combined_df_2.drop_duplicates(subset=["time", "host_id"], keep="first")

    return combined_df_1, combined_df_2

def do_overlap(df_1, df_2, aggergator, bottom_percentile, top_percentile):
    column_name = "mean_packets_sent_push_messages_count"
    data_name = aggregator + "_packets_sent_push_messages_count"
    if aggregator == "median":
        aggregated_df = df_1.groupby('time')[column_name].median().reset_index()
    else:
        aggregated_df = df_1.groupby('time')[column_name].mean().reset_index()

    fig, ax1 = plt.subplots(figsize=(18, 7))
    color1 = 'blue'
    ax1.plot(aggregated_df['time'], aggregated_df[column_name], label=aggergator + ' Packets Sent Push Messages Count (' + str(bottom_percentile) + '-' + str(top_percentile) + '% stake)', color=color1)
    ax1.set_xlabel('Time')
    ax1.set_ylabel(aggergator + ' Packets Sent Push Messages Count', color=color1)
    ax1.tick_params(axis='y', labelcolor=color1)
    # ax1.set_ylim(25000, 29000)  # Set the maximum limit to 100

    # Create a second axis using twinx() to share the same x-axis
    ax2 = ax1.twinx()


    df_2.set_index("time", inplace=True)
    unique_hosts_count = df_2.resample('1H')['host_id'].nunique()
    # Plot dataframe 2 on the right y-axis
    color2 = 'green'
    ax2.plot(unique_hosts_count.index, unique_hosts_count.values, label='# of New Validators per Hour', color=color2, marker='o', linestyle='-', markersize=3)
    ax2.set_ylabel('Number of New Validators Started within each hour', color=color2)
    ax2.tick_params(axis='y', labelcolor=color2)

    # Add legend to distinguish between dataframes
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc='upper left')
    line_time = pd.to_datetime("2023-07-03 04:10:00")
    plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')
    line_time = pd.to_datetime("2023-07-10 21:22:00")
    plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
    line_time = pd.to_datetime("2023-07-17 18:44:00")
    plt.axvline(x=line_time, color='purple', linestyle='--', label='1.16.4 Activation Date')
    plt.grid()
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2)

    plot_title = 'Validator Egress Push Message Count Overlapped with # of Validators coming online'

    plot_name = './plots/packets_sent_push_messages_count_new_validator_count_' + aggergator + '_stake_' + str(bottom_percentile) + '-' + str(top_percentile) + '.png'
    # Plot the mean values
    plt.title(plot_title)
    plt.savefig(plot_name, dpi=300)

    plt.show()

def plot_top_N_movers(validator_df, data_type, top_N_host_ids, bottom_percentile, top_percentile, N):
    data_name = "mean_" + data_type
    validator_df = validator_df[validator_df['host_id'].isin(top_N_host_ids.index)]
    validator_df["time"] = pd.to_datetime(validator_df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")

    plt.figure(figsize=(18, 10))  # Width: 12 inches, Height: 6 inches
    plt.grid()
    line_time = pd.to_datetime("2023-07-03 04:10:00")
    plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')
    line_time = pd.to_datetime("2023-07-10 21:22:00")
    plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
    line_time = pd.to_datetime("2023-07-17 18:44:00")
    plt.axvline(x=line_time, color='green', linestyle='--', label='1.16.4 Activation Date')

    print(validator_df.head(10))

    groupby_obj = validator_df.groupby("host_id")
    for host_id, group in groupby_obj:
        # label = f"{host_id} ({group['activatedStake'].iloc[0]})"
        plt.plot(group["time"], group[data_name], label=host_id)

    plot_title = data_name + " for the Top " + str(N) + " host_ids with largest change in packets_sent_push_messages_count. stake range: " + str(bottom_percentile) + '-' + str(top_percentile) + '%'

    # plt.plot(validator_df['time'], validator_df[data_name])
    plt.ylim(ymax=2500000)
    plt.xlabel('Time')
    plt.ylabel(data_name + '(us)')
    plt.title(plot_title)
    plt.legend(title="Host ID", loc='upper center', bbox_to_anchor=(.5, -0.1), ncol=3)
    plt.tight_layout()
    plt.savefig('./plots/' + plot_title + '.png', dpi=300)

    plt.show()

def get_N(df_1, df_2, which_N, N):
    top_N_host_ids = get_top_N_largest_engress_by_host_id(df_1, N)
    df_1["time"] = pd.to_datetime(df_1["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    df_2["time"] = pd.to_datetime(df_2["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")

    if which_N == "top":
        df_1 = df_1[df_1['host_id'].isin(top_N_host_ids.index)]
        df_2 = df_2[df_2['host_id'].isin(top_N_host_ids.index)]
    else:
        df_1 = df_1[~df_1['host_id'].isin(top_N_host_ids.index)]
        df_2 = df_2[~df_2['host_id'].isin(top_N_host_ids.index)]

    return df_1, df_2

def create_ratio_df(push_df, pull_df, which_N, N):
    if which_N == "top" or which_N == "bottom":
        push_df, pull_df = get_N(push_df, pull_df, which_N, N)

    # Merge the two dataframes on 'time' and 'host_id' to get overlapping data
    merged_df = push_df.merge(pull_df, on=['time', 'host_id'], how='inner')

    # Calculate the division of push_messages by pull_messages
    merged_df['push_pull_ratio'] = merged_df['mean_packets_sent_push_messages_count'] / merged_df['mean_new_pull_requests_count']

    # Drop any redundant columns if needed
    merged_df.drop(columns=['mean_packets_sent_push_messages_count', 'mean_new_pull_requests_count'], inplace=True)

    return merged_df

def plot_ratio(push_df, pull_df, bottom_percentile, top_percentile, plot_top_N, N):
    merged_df = create_ratio_df(push_df, pull_df, plot_top_N, N)

    plt.figure(figsize=(18, 10))  # Width: 12 inches, Height: 6 inches
    plt.grid()
    line_time = pd.to_datetime("2023-07-03 04:10:00")
    plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')
    line_time = pd.to_datetime("2023-07-10 21:22:00")
    plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
    line_time = pd.to_datetime("2023-07-17 18:44:00")
    plt.axvline(x=line_time, color='green', linestyle='--', label='1.16.4 Activation Date')

    grouped = merged_df.groupby('host_id')
    for host_id, group in grouped:
        plt.plot(group['time'], group['push_pull_ratio'], label=f'{host_id}')

    plt.xlabel('Time')
    plt.ylabel("ratio push/pull requests")
    plot_title = "ratio of push to pull requests. stake range: " + str(bottom_percentile) + '-' + str(top_percentile) + '%'
    plt.title(plot_title)
    plt.legend(title="Host ID", loc='upper center', bbox_to_anchor=(.5, -0.1), ncol=3)
    plt.tight_layout()
    plt.savefig('./plots/' + plot_title + '.png', dpi=300)

    plt.show()
    


if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("error. need to pass in top and bottom percentile: python main.py <bottom> <top>")
        sys.exit(-1)

    data_type_movers = sys.argv[1]
    data_type_results = sys.argv[2] # vanilla or actual data_type. vanilla prints aggregate
    bottom_percentile = int(sys.argv[3])
    top_percentile = int(sys.argv[4])
    N = int(sys.argv[5]) # top N to get, N == -1, plot all
    aggregator = sys.argv[6] # mean or median

    if aggregator not in aggregator_types:
        print("invalid aggregator passed in: " + aggregator)
        sys.exit(-1)
    
    movers_df = TransformData.loadData(data_type_movers)
    results_df = None if data_type_results == "vanilla" else TransformData.loadData(data_type_results)
    stakes_df = TransformData.loadStakes(MINIMUM_VALIDATOR_VERSION)
    df = TransformData.mergeDataframes(movers_df, stakes_df)
    df = TransformData.getDataframePercentile(df, bottom_percentile, top_percentile)

    if not results_df:
        plot = Plotter(data_type_movers, (bottom_percentile, top_percentile))
        plot_type = sys.argv[7]
        if plot_type == "aggregate":
            df = TransformData.aggregate_data_frame_by(df, data_type_movers, aggregator)
        plot.plot_2(df, plot_type)

    sys.exit(0)


    data = load_data(data_type_movers)
    stakes = get_validator_stakes()
    df = merge_dataframes(data, stakes)
    percentile_df = get_dataframe_percentile(df, bottom_percentile, top_percentile)
    

    if data_type_results == "vanilla":
        plot_type = sys.argv[7] # individual or aggregate
        plot_title = str(bottom_percentile) + "-" + str(top_percentile) + "%-ile by stake"
        if plot_type == "aggregate":
            aggregate_data_frame_by(percentile_df, data_type_movers, aggregator)
        if plot_type == "individual":
            plot_dataframe(percentile_df, plot_title, data_type_movers, False)
        else:
            unique_host_ids = percentile_df['host_id'].unique()
            print_query(data_type_movers, unique_host_ids)
            plot_by_time_aggregator(percentile_df, data_type_movers, aggregator, bottom_percentile, top_percentile)

    sys.exit(0)

    if data_type == "overlap":
        df_1, df_2 = get_validator_data_overlap(data_type)
        stakes = get_validator_stakes()
        print("total nodes: " + str(len(stakes)))
        df_1 = merge_dataframes(df_1, stakes)
        percentile_df = get_dataframe_percentile(df_1, bottom_percentile, top_percentile)
        do_overlap(percentile_df, df_2, aggregator, bottom_percentile, top_percentile)
        sys.exit(0)

    if data_type == "topN":
        df_1, df_2 = get_validator_data_overlap("packets_sent_push_messages_count")
        stakes = get_validator_stakes()
        print("total nodes: " + str(len(stakes)))
        df_1 = merge_dataframes(df_1, stakes)
        validator_new_data = get_validator_data("validator")
        percentile_df = get_dataframe_percentile(df_1, bottom_percentile, top_percentile)
        plot_top_N_largest_increases_in_egress(percentile_df, validator_new_data, int(N), bottom_percentile, top_percentile)
        sys.exit(0)

    if data_type == "validator_restarts":
        df_1, df_2 = get_validator_data_overlap("packets_sent_push_messages_count")
        validator_new_data = get_validator_data(data_type)
        stakes = get_validator_stakes()
        df_1 = merge_dataframes(df_1, stakes)
        percentile_df = get_dataframe_percentile(df_1, bottom_percentile, top_percentile)
        do_validator_restarts(percentile_df, validator_new_data, int(N))
        sys.exit(0)

    if data_type == "topN_validator_restarts":
        df_1, df_2 = get_validator_data_overlap("packets_sent_push_messages_count")
        validator_new_data = get_validator_data(data_type)
        stakes = get_validator_stakes()
        df_1 = merge_dataframes(df_1, stakes)
        percentile_df = get_dataframe_percentile(df_1, bottom_percentile, top_percentile)
        get_top_N_restarted_validators(percentile_df, validator_new_data, int(N))
        sys.exit(0)

    # 1) packets_sent_push_messages_count and mean_new_pull_requests_count
    if data_type == "ratio":
        stakes = get_validator_stakes()
        push_df, _ = get_validator_data_overlap("packets_sent_push_messages_count_abridged")
        pull_df = get_validator_data("new_pull_requests_count")

        push_df = merge_dataframes(push_df, stakes)
        pull_df = merge_dataframes(pull_df, stakes)

        push_df = get_dataframe_percentile(push_df, bottom_percentile, top_percentile)
        pull_df = get_dataframe_percentile(pull_df, bottom_percentile, top_percentile)
        
        plot_ratio(push_df, pull_df, bottom_percentile, top_percentile, plot_top_N, int(N))
        sys.exit(0)

    if data_type == "ratio-aggregate":
        stakes = get_validator_stakes()
        push_df, _ = get_validator_data_overlap("packets_sent_push_messages_count_abridged")
        pull_df = get_validator_data("new_pull_requests_count")

        push_df = merge_dataframes(push_df, stakes)
        pull_df = merge_dataframes(pull_df, stakes)

        push_df = get_dataframe_percentile(push_df, bottom_percentile, top_percentile)
        pull_df = get_dataframe_percentile(pull_df, bottom_percentile, top_percentile)
        
        ratio_df = create_ratio_df(push_df, pull_df, "top", int(N))
        plot_by_time_aggregator(ratio_df, "push_pull_ratio", aggregator, bottom_percentile, top_percentile, "top", int(N))
                
        ratio_df = create_ratio_df(push_df, pull_df, "bottom", int(N))
        plot_by_time_aggregator(ratio_df, "push_pull_ratio", aggregator, bottom_percentile, top_percentile, "bottom", len(stakes) - int(N))

        sys.exit(0)




    data = get_validator_data(data_type)
    stakes = get_validator_stakes()
    print("total nodes: " + str(len(stakes)))

    df = merge_dataframes(data, stakes)

    if data_type == "validator_new":
        do_validator_new(data)
        sys.exit(0)


    percentile_df = get_dataframe_percentile(df, bottom_percentile, top_percentile)
    plot_title = str(bottom_percentile) + "-" + str(top_percentile) + "%-ile by stake"
    if plot_flag:
        plot_dataframe(percentile_df, plot_title, data_type, False)
    unique_host_ids = percentile_df['host_id'].unique()
    print_query(data_type, unique_host_ids)


    if plot_top_N:
        egress_message_df, _ = get_validator_data_overlap("packets_sent_push_messages_count")
        stakes = get_validator_stakes()
        egress_message_df = merge_dataframes(egress_message_df, stakes)
        egress_message_df = get_dataframe_percentile(egress_message_df, bottom_percentile, top_percentile)
        top_N_host_ids = get_top_N_largest_engress_by_host_id(egress_message_df, int(N))
        plot_top_N_movers(percentile_df, data_type, top_N_host_ids, bottom_percentile, top_percentile, int(N))
    else:
        plot_by_time_aggregator(percentile_df, data_type, aggregator, bottom_percentile, top_percentile)



    
