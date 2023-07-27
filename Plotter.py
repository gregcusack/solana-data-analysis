import matplotlib.pyplot as plt
import pandas as pd

class Plotter:
    def __init__(self, data_type, percentiles: tuple, action, *args):
        self.data_name = "mean_" + data_type
        self.bottom_percentile, self.top_percentile = percentiles
        self.plot_title = self.data_name + " - " + str(self.bottom_percentile) + "-" + str(self.top_percentile) + "%-ile by stake. "
        if args is not None and args[0] > 0:
            N = args[0]
            data_type_movers = args[1]
            if action == "topN_inverse":
                self.plot_title += "All Validators excluding "
            self.plot_title += "Top " + str(N) + " " + data_type_movers + " Movers"

    def plot(self, df, plot_type, aggregator, data_name):
        plt.figure(figsize=(18, 10))  # Width: 12 inches, Height: 6 inches
        print(df.head(10))
        if plot_type == "individual":
            lines = []
            groupby_obj = df.groupby("host_id")
            for host_id, group in groupby_obj:
                # label = f"{host_id} ({group['activatedStake'].iloc[0]})"
                label = f"{host_id})"
                line, = plt.plot(group["time"], group[data_name], label=label)
                lines.append(line)
        else:
            plt.plot(df['time'], df[data_name])
        
        plt.grid()
        line_time = pd.to_datetime("2023-07-03 04:10:00")
        plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')
        line_time = pd.to_datetime("2023-07-10 21:22:00")
        plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
        line_time = pd.to_datetime("2023-07-17 18:44:00")
        plt.axvline(x=line_time, color='green', linestyle='--', label='1.16.4 Activation Date')
        plt.xlabel('Time')
        plt.ylabel(self.data_name)
        # Set the y-axis limits
        # plt.ylim(ymin=665000)
        self.plot_title += ". " + plot_type
        if plot_type == "aggregate":
            self.plot_title += "-" + aggregator
        plt.title(self.plot_title)
        plt.legend(title='Host ID', bbox_to_anchor=(0.5, -0.05), loc='upper center', ncol=3)
        plt.tight_layout()
        if "/" in self.plot_title:
            self.plot_title = self.plot_title.replace("/", "_div_")
        plt.savefig('./plots/' + self.plot_title + '.png', dpi=300)

        plt.show()