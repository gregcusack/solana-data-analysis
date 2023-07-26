import matplotlib.pyplot as plt
import mplcursors
import pandas as pd

class Plotter:
    def __init__(self, data_type, percentiles: tuple):
        self.data_type = data_type
        self.data_name = "mean_" + self.data_type
        self.bottom_percentile, self.top_percentile = percentiles
        self.plot_title = self.data_name + ": " + str(self.bottom_percentile) + "-" + str(self.top_percentile) + "%-ile by stake"

        # self.plt = plt

    def plot_2(self, df, plot_type):
        plt.figure(figsize=(18, 10))  # Width: 12 inches, Height: 6 inches
        if plot_type == "individual":
            lines = []
            groupby_obj = df.groupby("host_id")
            for host_id, group in groupby_obj:
                label = f"{host_id} ({group['activatedStake'].iloc[0]})"
                line, = plt.plot(group["time"], group[self.data_name], label=label)
                lines.append(line)
        else:
            plt.plot(df['time'], df[self.data_name])
        
        plt.grid()
        line_time = pd.to_datetime("2023-07-03 04:10:00")
        plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')
        # if data_type == "packets_sent_push_messages_count":
        line_time = pd.to_datetime("2023-07-10 21:22:00")
        plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
        line_time = pd.to_datetime("2023-07-17 18:44:00")
        plt.axvline(x=line_time, color='green', linestyle='--', label='1.16.4 Activation Date')

        plt.xlabel('Time')
        plt.ylabel(self.data_name)
        plt.title(self.plot_title)
        plt.savefig('./plots/' + self.plot_title + '.png', dpi=300)

        plt.show()

    def plot(self, df):
        data_name = "mean_" + self.data_type
        plt.figure(figsize=(18, 10))  # Width: 12 inches, Height: 6 inches
        lines = []
        try:
            groupby_obj = df.groupby("host_id")
            for host_id, group in groupby_obj:
                label = f"{host_id} ({group['activatedStake'].iloc[0]})"
                line, = plt.plot(group["time"], group[data_name], label=label)
                lines.append(line)
        except KeyError as e:
            print("no host_id in df. attempting to plot aggregate plot")

        line_time = pd.to_datetime("2023-07-03 04:10:00")
        plt.axvline(x=line_time, color='red', linestyle='--', label='1.16.2 Activation Date')
        line_time = pd.to_datetime("2023-07-10 21:22:00")
        plt.axvline(x=line_time, color='orange', linestyle='--', label='1.16.3 Activation Date')
        line_time = pd.to_datetime("2023-07-17 18:44:00")
        plt.axvline(x=line_time, color='green', linestyle='--', label='1.16.4 Activation Date')

        # Set the x-axis label
        plt.xlabel("time")

        # Set the y-axis label
        plt.ylabel(data_name)
        
        # Place the legend below the graph
        plt.legend(bbox_to_anchor=(0.5, -0.05), loc="upper center", ncol=3, frameon=False, fontsize='small')

        # if set_y_axis_limits:
        #     y_min = 9000
        #     y_max = 23000
        #     # Set the y-axis limits
        #     plt.ylim(y_min, y_max)

        # Adjust the layout to accommodate the legend
        plt.subplots_adjust(bottom=0.25)
        plt.grid()

        # # Set the chart title
        # plt.title("Packets Sent Push Messages Count. " + str(bottom_percentile) + "-" + str(top_percentile) + "%-ile by stake")
        plt.title(data_name + ": " + self.plot_title)

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