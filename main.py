import pandas as pd
import json
import matplotlib.pyplot as plt
import sys

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

def validator_messages():
    df = pd.read_csv ('../7_2_4am-7_4_4am_1hr_packets_sent_push_messages_count_no_version.csv')
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    return df

def validator_stakes():
    data = json.load(open('../validator_stakes.json'))
    stakes = pd.DataFrame(data["validators"])
    stakes = stakes.drop(stake_columns_to_drop, axis=1)

    stakes_filtered = stakes[stakes['version'] == '1.16.2']
    stakes_filtered = stakes_filtered.sort_values(by='activatedStake')
    stakes_filtered['activatedStake'] = stakes_filtered['activatedStake'].div(1000000000).round(9)
    stakes_filtered.rename(columns = {'identityPubkey':'host_id'}, inplace = True)

    return stakes_filtered

def merge_dataframes(df1, df2):
    df1.info()
    df2.info()
    all_data = pd.merge(df1, df2, on="host_id", how="inner")
    print(all_data)
    print(all_data.iloc[21982])

    return all_data

def plot_dataframe(df):
    # Group the data by "host_id" and plot each group
    df = df.groupby("host_id")
    for host_id, group in df:
        plt.plot(group["time"], group["mean_packets_sent_gossip_requests_count"], label=host_id)

    # Set the x-axis label
    plt.xlabel("time")

    # Set the y-axis label
    plt.ylabel("mean_packets_sent_gossip_requests_count")

    # Add a legend to identify each line
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")

    # Display the plot
    plt.show()

def plot_dataframe_percentile(df, bottom_percentile, top_percentile):
    # Calculate the threshold for the top 10% of "activatedStake"
    # top_n_threshold = 1 - df["activatedStake"].quantile(1-percentile)

    # # Filter the data for host_ids in the top 10%
    # df = df[df["activatedStake"] >= top_n_threshold]

    bottom_threshold = df["activatedStake"].quantile(bottom_percentile/100)
    top_threshold = df["activatedStake"].quantile(top_percentile/100)

    df = df[df["activatedStake"].between(bottom_threshold, top_threshold)]

    # Group the data by "host_id" and plot each group
    groupby_obj = df.groupby("host_id")
    for host_id, group in groupby_obj:
        label = f"{host_id} ({group['activatedStake'].iloc[0]})"
        plt.plot(group["time"], group["mean_packets_sent_gossip_requests_count"], label=label)

    # for index, col in df.iterrows():
    #     if col['host_id'] == '5D1fNXzvv5NjV1ysLjirC4WY92RNsVH18vjmcszZd8on':
    #         print(col)

    ##################
    line_time = pd.to_datetime("2023-07-03 04:10:00")
    plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')

    # Set the x-axis label
    plt.xlabel("time")

    # Set the y-axis label
    plt.ylabel("mean_packets_sent_gossip_requests_count")
    
    try:
        # Get the current y-axis tick values
        _, yticks = plt.yticks()
        yticks = pd.to_numeric([ytick.get_text() for ytick in yticks])


        # Add horizontal tick bars at y-axis tick positions
        for ytick in yticks:
            plt.axhline(y=ytick, color='gray', linestyle='dotted')
    except ValueError as e:
        print("error: " + str(e))

    # Add a legend to identify each line
    # plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    # Place the legend below the graph
    plt.legend(bbox_to_anchor=(0.5, -0.15), loc="upper center", ncol=2, frameon=False)

    # Adjust the layout to accommodate the legend
    plt.subplots_adjust(bottom=0.25)

    # Set the chart title
    plt.title("Packets Sent Push Messages Count. " + str(bottom_percentile) + "-" + str(top_percentile) + "%-ile by stake")

    df = df.drop_duplicates(subset=['host_id'])
    for index, col in df.iterrows():
        print(col['host_id'])


    # Display the plot
    plt.show()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("error. need to pass in top and bottom percentile: python main.py <bottom> <top>")
        sys.exit(-1)

    bottom_percentile = int(sys.argv[1])
    top_percentile = int(sys.argv[2])

    egress_messages = validator_messages()
    stakes = validator_stakes()

    df = merge_dataframes(egress_messages, stakes)
    df.info()

    # # plot_dataframe(df)

    plot_dataframe_percentile(df, bottom_percentile, top_percentile)


