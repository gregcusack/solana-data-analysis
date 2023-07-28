import os
import pandas as pd
import json

stake_columns_to_drop = [
    "lastVote", 
    "rootSlot", 
    "credits", 
    "epochCredits", 
    "delinquent", 
    "skipRate", 
    "commission"
]

class TransformData:
    @staticmethod
    def loadData(data_type):
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
    
    @staticmethod
    def loadStakes(minimum_validator_version):
        data = json.load(open('../data/validator_stakes_new.json'))
        stakes = pd.DataFrame(data["validators"])
        stakes = stakes.drop(stake_columns_to_drop, axis=1)
        stakes['version_tuple'] = stakes['version'].apply(lambda x: tuple(map(int, x.split('.'))) if x != "unknown" else None)

        # Filter out rows with a version number less than the target version
        stakes_filtered = stakes[stakes['version_tuple'] >= minimum_validator_version]

        # Drop the temporary version_tuple column if you don't need it anymore
        stakes_filtered = stakes_filtered.drop(columns=['version_tuple'])

        stakes_filtered = stakes_filtered.sort_values(by='activatedStake')
        stakes_filtered['activatedStake'] = stakes_filtered['activatedStake'].div(1000000000).round(9)
        stakes_filtered.rename(columns = {'identityPubkey':'host_id'}, inplace = True)

        return stakes_filtered

    @staticmethod
    def mergeDataframes(df1, df2):
        return pd.merge(df1, df2, on="host_id", how="inner")
    
    @staticmethod
    def getDataframePercentile(df, bottom_percentile, top_percentile):
        df['percent_rank'] = df['activatedStake'].rank(pct=True)

        df_filtered = df[(df['percent_rank'] >= bottom_percentile / 100) & (df['percent_rank'] < top_percentile / 100)]

        unique_host_ids = df_filtered['host_id'].unique()
        print("Unique Host IDs in percentile: " + str(len(unique_host_ids)))

        return df_filtered
    
    @staticmethod
    def aggregate_data_frame_by(df, data_type: str, aggregator: str):
        if data_type == "ratio":
            column_name = "ratio"
        elif data_type == "validator_new":
            df.set_index("time", inplace=True)
            return df.resample('1H')['host_id'].nunique()
        else:
            column_name = "mean_" + data_type
        if aggregator == "median":
            aggregated_df = df.groupby('time')[column_name].median().reset_index()
        else:
            aggregated_df = df.groupby('time')[column_name].mean().reset_index()
        return aggregated_df

    @staticmethod
    def get_top_N_largest_movers_by_host_id(df, data_type, N):
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

        return top_N_host_ids.index
    
    @staticmethod
    def get_top_N_largest_mean_data_by_host_id(df, data_type, N):
        column_name = "mean_" + data_type

        mean_data = df.groupby('host_id')[column_name].mean()
        mean_data = mean_data.sort_values(ascending=False).head(N)
        print(mean_data.head(N))
        return mean_data.index
    
    @staticmethod
    def get_data_for_specific_host_ids(top_N_host_ids, results_df, include_top_N):
        if include_top_N:
            return results_df[results_df['host_id'].isin(top_N_host_ids)]
        else:
            return results_df[~results_df['host_id'].isin(top_N_host_ids)]
        
    @staticmethod
    def create_ratio_df(num_df, denom_df, data_type_num, data_type_denom):
        data_type_num = "mean_" + data_type_num
        data_type_denom = "mean_" + data_type_denom
        # Merge the two dataframes on 'time' and 'host_id' to get overlapping data
        merged_df = num_df.merge(denom_df, on=['time', 'host_id'], how='inner')

        # Calculate the division of push_messages by pull_messages
        merged_df['ratio'] = merged_df[data_type_num] / merged_df[data_type_denom]

        # Drop any redundant columns if needed
        merged_df.drop(columns=[data_type_num, data_type_denom], inplace=True)

        return merged_df

    @staticmethod
    def generate_validator_restart_counts(df):
        df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
        validator_new_df = df[['time', 'host_id']]

        hourly_counts = validator_new_df.groupby('host_id').resample('1H', on="time").count()
        hourly_counts = hourly_counts.loc[(hourly_counts != 0).any(axis=1)]
        # Unstack the 'host_id' index to create separate columns for each host_id
        hourly_counts = hourly_counts.unstack('host_id')
        return hourly_counts
    

    @staticmethod
    def print_top_N_host_ids(df, top_N_host_ids):
        print("Top N Host IDs by Stake")
        df['percent_rank'] = df['activatedStake'].rank(pct=True) * 100
        top_N_df = df[df['host_id'].isin(top_N_host_ids)]

        top_N_df = top_N_df.drop_duplicates(subset='host_id')

        for _, row in top_N_df.iterrows():
            host_id = row['host_id']
            activated_stake = row['activatedStake']
            percentile_rank = row['percent_rank']

            print(f"Host ID: {host_id}, Activated Stake: {activated_stake:.2f}, Percentile Rank: {percentile_rank:.2f}%")
