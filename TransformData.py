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
        column_name = "mean_" + data_type
        if aggregator == "median":
            aggregated_df = df.groupby('time')[column_name].median().reset_index()
        else:
            aggregated_df = df.groupby('time')[column_name].mean().reset_index()
        return aggregated_df
