import pandas as pd
import json
import matplotlib.pyplot as plt
import sys
import mplcursors

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
    elif data_type == "validator_new":
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
        return combined_df
    else:
        # file_name = "../data/last_30_days_" + data_type + "_7_2_7_16_1hr.csv" 
        file_name = "../data/last_30_days_" + data_type + "_7_6_7_21_1hr.csv" 
        # file_name = "../data/" + data_type + "_last_30_days_until_7_10_4pm_1hr.csv" 


    # df = pd.read_csv ('../data/" + data_type + "packets_sent_gossip_requests_count_7_2_4am-7_4_4am_1hr.csv')
    df = pd.read_csv(file_name)
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    return df

def get_validator_stakes():
    # data = json.load(open('../data/validator_stakes.json'))
    data = json.load(open('../data/validator_stakes_new.json'))
    stakes = pd.DataFrame(data["validators"])
    # stakes = stakes.drop(stake_columns_to_drop, axis=1)
    stakes = stakes.drop(stake_columns_to_drop, axis=1)
    # stakes_filtered = stakes[stakes['version'] == '1.16.2']
    print(stakes['version'])

    # stakes['version_tuple'] = stakes['version'].str.split('.').apply(lambda x: tuple(map(int, x)))
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

    bottom_threshold = df["activatedStake"].quantile(bottom_percentile/100)
    top_threshold = df["activatedStake"].quantile(top_percentile/100)

    df = df[df["activatedStake"].between(bottom_threshold, top_threshold)]
    
    unique_host_ids = df['host_id'].unique()
    print("Unique Host IDs in percentile: " + str(len(unique_host_ids)))

    return df

def get_dataframe_percentile_2(df, bottom_percentile, top_percentile):
    # Calculate the threshold for the top 10% of "activatedStake"

    df['percent_rank'] = df['activatedStake'].rank(pct=True)

    df_filtered = df[(df['percent_rank'] >= bottom_percentile / 100) & (df['percent_rank'] < top_percentile / 100)]
    print(df_filtered)


    # bottom_threshold = df["activatedStake"].quantile(bottom_percentile/100)
    # top_threshold = df["activatedStake"].quantile(top_percentile/100)

    # df = df[df["activatedStake"].between(bottom_threshold, top_threshold)]
    
    unique_host_ids = df_filtered['host_id'].unique()
    print("Unique Host IDs in percentile: " + str(len(unique_host_ids)))

    return df_filtered

def plot_dataframe(df, plot_title, data_type, set_y_axis_limits):
    # Group the data by "host_id" and plot each group
    data_name = "mean_" + data_type
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

def plot_by_time_aggregator(df, data_type, aggregator, bottom_percentile, top_percentile):
    column_name = "mean_" + data_type
    data_name = aggregator + "_" + data_type
    if aggregator == "median":
        aggregated_df = df.groupby('time')[column_name].median().reset_index()
    else:
        aggregated_df = df.groupby('time')[column_name].mean().reset_index()

    plt.figure(figsize=(18, 7))  # Width: 12 inches, Height: 6 inches
    plt.grid()
    line_time = pd.to_datetime("2023-07-03 04:10:00")
    plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')
    if data_type == "packets_sent_push_messages_count":
        line_time = pd.to_datetime("2023-07-10 21:22:00")
        plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
        line_time = pd.to_datetime("2023-07-17 18:44:00")
        plt.axvline(x=line_time, color='green', linestyle='--', label='1.16.4 Activation Date')


    # plt.ylim(30000, 38000)  # Set the maximum limit to 100
    plot_title = data_name + '_' + str(bottom_percentile) + '-' + str(top_percentile) + '% stake'

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
    plt.ylabel("unique host_id count")
    plt.title(plot_title)
    plt.savefig('./plots/' + plot_title + '.png', dpi=300)

    plt.show()

def get_validator_data_overlap(data_type):
    data_type = "packets_sent_push_messages_count"
    file_name_1 = "../data/"+ data_type + "_last_30_days_until_7_10_4pm_1hr.csv"
    file_name_2 = "../data/"+ data_type + "_last_30_days_until_7_18_6pm_1hr.csv"
    file_name_3 = "../data/last_30_days_" + data_type + "_7_6_7_21_1hr.csv" 
    df_1 = pd.read_csv(file_name_1)
    df_2 = pd.read_csv(file_name_2)
    df_3 = pd.read_csv(file_name_3)
    df_1["time"] = pd.to_datetime(df_1["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    df_2["time"] = pd.to_datetime(df_2["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    df_3["time"] = pd.to_datetime(df_3["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    combined_df_1 = pd.concat([df_1, df_2, df_3])
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
    

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("error. need to pass in top and bottom percentile: python main.py <bottom> <top>")
        sys.exit(-1)

    data_type = sys.argv[1]
    bottom_percentile = int(sys.argv[2])
    top_percentile = int(sys.argv[3])
    percentile = float(sys.argv[4])
    plot_flag = sys.argv[5].lower() == 'true'
    aggregator = sys.argv[6] # mean or median
    print(plot_flag)    

    host_id = ''
    if len(sys.argv) == 8:
        host_id = sys.argv[7]

    if aggregator not in aggregator_types:
        print("invalid aggregator passed in: " + aggregator)
        sys.exit(-1)

    if data_type == "overlap":
        df_1, df_2 = get_validator_data_overlap(data_type)
        stakes = get_validator_stakes()
        print("total nodes: " + str(len(stakes)))
        df_1 = merge_dataframes(df_1, stakes)
        percentile_df = get_dataframe_percentile_2(df_1, bottom_percentile, top_percentile)
        do_overlap(percentile_df, df_2, aggregator, bottom_percentile, top_percentile)
        sys.exit(0)

    data = get_validator_data(data_type)
    stakes = get_validator_stakes()
    print("total nodes: " + str(len(stakes)))

    df = merge_dataframes(data, stakes)


    if data_type == "validator_new":
        do_validator_new(data)
        sys.exit(0)

    # percentile_df = get_dataframe_percentile(df, bottom_percentile, top_percentile)
    percentile_df = get_dataframe_percentile_2(df, bottom_percentile, top_percentile)
    plot_title = str(bottom_percentile) + "-" + str(top_percentile) + "%-ile by stake"
    if plot_flag:
        plot_dataframe(percentile_df, plot_title, data_type, False)
    unique_host_ids = percentile_df['host_id'].unique()
    print_query(data_type, unique_host_ids)

    plot_by_time_aggregator(percentile_df, data_type, aggregator, bottom_percentile, top_percentile)



    # df_post_activation = get_df_post_activation(percentile_df)

    # to_drop_if_nan = "mean_" + data_type
    # df_post_activation = df_post_activation.dropna(subset=[to_drop_if_nan])

    # # increased_df_2 = find_large_changes_in_data_between_ends(percentile_df, percentile, data_type)
    # increased_df_2 = find_large_changes_in_data_between_ends(df_post_activation, percentile, data_type)
    # increased_df_3 = find_large_changes_in_data_between_ends(percentile_df, percentile, data_type)



    # # union_df = pd.concat([increased_df_1, increased_df_2])
    # # union_df = increased_df_1.merge(increased_df_2, on='host_id', how='inner')
    # union_df = merge_dataframes(increased_df_2, increased_df_3)

    # # union_df = union_df.sort_values(by='time')
    # # unique_host_ids = union_df['host_id'].unique()
    # unique_host_ids = union_df['host_id'].unique()
    # print("Unique Host IDs that increased by " + str(percentile) + "%: " + str(len(unique_host_ids)))
    
    # print_query(data_type, unique_host_ids)
    # # for host_id_val in unique_host_ids:
    # #     print(host_id_val)
    # if plot_flag:
    #     plot_dataframe(increased_df_2, plot_title, data_type, False)
    #     plot_dataframe(increased_df_3, plot_title, data_type, False)

    # print("------------------------------")
    # unique_host_ids_2 = increased_df_2['host_id'].unique()
    # print_query(data_type, unique_host_ids_2)

    # print("------------------------------")
    # unique_host_ids_3 = increased_df_3['host_id'].unique()
    # print_query(data_type, unique_host_ids_3)






    # df = df[df['host_id'].isin(unique_host_ids)]
    # plot_title = "Host IDs in " + plot_title + " that increased by more than " + str(percentile) + "% after 1.16.2 activation"
    # if plot_flag:
    #     plot_dataframe(df, plot_title, data_type, False)
    # # plot_dataframe(union_df, plot_title, data_type, False)


    # if host_id != '' and plot_flag:
    #     plot_title = "Host ID: " + host_id
    #     df_specific_host = df[df['host_id'] == host_id].copy()
    #     plot_dataframe(df_specific_host, plot_title, data_type, False)
