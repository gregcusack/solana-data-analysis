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

def get_validator_data(data_type):
    # file_name = "../data/"+ data_type + "_7_2_4am_7_6_4am_1hr.csv"
    file_name = "../data/"+ data_type + "_last_30_days_until_7_10_4pm_1hr.csv"

    # df = pd.read_csv ('../data/" + data_type + "packets_sent_gossip_requests_count_7_2_4am-7_4_4am_1hr.csv')
    df = pd.read_csv(file_name)
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    return df

def get_validator_stakes():
    data = json.load(open('../data/validator_stakes.json'))
    stakes = pd.DataFrame(data["validators"])
    stakes = stakes.drop(stake_columns_to_drop, axis=1)

    stakes_filtered = stakes[stakes['version'] == '1.16.2']
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

def find_large_changes_in_data_between_points(df, percentile, data_type):
    data_name = "mean_" + data_type
    percentage_threshold = percentile  # Desired percentage increase threshold
    start_datetime = pd.to_datetime("2023-07-03 04:10:00")

    # Calculate the percentage change within each host_id group
    df['percentage_change'] = df.groupby('host_id')[data_name].pct_change() * 100

    # Filter the dataframe based on the desired percentage increase threshold
    increased_df = df[df['percentage_change'] >= percentage_threshold]
    # increased_df = df[(df['percentage_change'] >= percentage_threshold) & (df['time'] >= start_datetime)]


    # Get the unique host_ids with increased stake
    unique_host_ids = increased_df['host_id'].unique()
    # print(unique_host_ids)
    # print("Unique Host IDs that increased by " + str(percentile) + "%: " + str(len(unique_host_ids)))


    df_filtered = df[df['host_id'].isin(unique_host_ids)].copy()

    df_filtered = run_end_check_filtering(df_filtered, percentile * 0.6, data_name)
    # df_filtered = run_end_check_filtering(df_filtered, 3, data_name)


    unique_host_ids = df_filtered['host_id'].unique()
    print("Unique Host IDs that increased by " + str(percentile) + "%: " + str(len(unique_host_ids)))


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

    ending = "GROUP BY time(1h), \"host_id\" FILL(null)"

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

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("error. need to pass in top and bottom percentile: python main.py <bottom> <top>")
        sys.exit(-1)

    data_type = sys.argv[1]
    bottom_percentile = int(sys.argv[2])
    top_percentile = int(sys.argv[3])
    percentile = float(sys.argv[4])

    host_id = ''
    if len(sys.argv) == 6:
        host_id = sys.argv[5]

    data = get_validator_data(data_type)
    stakes = get_validator_stakes()

    df = merge_dataframes(data, stakes)

    percentile_df = get_dataframe_percentile(df, bottom_percentile, top_percentile)
    plot_title = str(bottom_percentile) + "-" + str(top_percentile) + "%-ile by stake"
    plot_dataframe(percentile_df, plot_title, data_type, False)

    df_post_activation = get_df_post_activation(percentile_df)

    # increased_df_1 = find_large_changes_in_data_between_points(df_post_activation, percentile, data_type)
    # plot_title = "Host IDs in " + plot_title + " that increased by more than " + str(percentile) + "% after 1.16.2 activation"
    # plot_dataframe(increased_df_1, plot_title, False)

    host_id_to_check = '3si45SHHXsP8C6PVo1Zcpcry7DuivvogscAA63D8AKmR'
    # if df['host_id'].isin([host_id_to_check]).any():
    #     print(host_id_to_check + " in large changes betw. points")

    # increased_df_2 = find_large_changes_in_data_between_ends(percentile_df, percentile, data_type)
    increased_df_2 = find_large_changes_in_data_between_ends(df_post_activation, percentile, data_type)
    increased_df_3 = find_large_changes_in_data_between_ends(percentile_df, percentile, data_type)

    # plot_title = "Host IDs in " + plot_title + " that increased by more than " + str(percentile) + "% after 1.16.2 activation"
    # plot_dataframe(increased_df_2, plot_title, False)

    if df['host_id'].isin([host_id_to_check]).any():
        print(host_id_to_check + " in large changes betw ends")

    # union_df = pd.concat([increased_df_1, increased_df_2])
    # union_df = increased_df_1.merge(increased_df_2, on='host_id', how='inner')
    union_df = merge_dataframes(increased_df_2, increased_df_3)

    # union_df = union_df.sort_values(by='time')
    # unique_host_ids = union_df['host_id'].unique()
    unique_host_ids = union_df['host_id'].unique()
    print(type(unique_host_ids))
    print("Unique Host IDs that increased by " + str(percentile) + "%: " + str(len(unique_host_ids)))
    
    print_query(data_type, unique_host_ids)
    # for host_id_val in unique_host_ids:
    #     print(host_id_val)

    plot_dataframe(increased_df_2, plot_title, data_type, False)
    plot_dataframe(increased_df_3, plot_title, data_type, False)

    print("------------------------------")
    unique_host_ids_2 = increased_df_2['host_id'].unique()
    print_query(data_type, unique_host_ids_2)

    print("------------------------------")
    unique_host_ids_3 = increased_df_3['host_id'].unique()
    print_query(data_type, unique_host_ids_3)






    df = df[df['host_id'].isin(unique_host_ids)]
    plot_title = "Host IDs in " + plot_title + " that increased by more than " + str(percentile) + "% after 1.16.2 activation"
    plot_dataframe(df, plot_title, data_type, False)
    # plot_dataframe(union_df, plot_title, data_type, False)


    if host_id != '':
        plot_title = "Host ID: " + host_id
        df_specific_host = df[df['host_id'] == host_id].copy()
        plot_dataframe(df_specific_host, plot_title, data_type, False)
