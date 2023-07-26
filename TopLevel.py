import os
import pandas as pd
from TransformData import *
from Plotter import *

class TopLevel:
    def __init__(self, data_type_movers, data_type_results, bottom_percentile, top_percentile):
        self.data_type_movers = data_type_movers
        self.data_type_results = data_type_results
        self.bottom_percentile = bottom_percentile
        self.top_percentile = top_percentile

        self.movers_df = None
        self.results_df = None
        self.stakes = None

        self.df = None

        self.plot = None

    def loadMovers(self):
        self.movers_df = TransformData.loadData(self.data_type_movers)

    def loadResults(self):
        self.results_df = TransformData.loadData(self.data_type_results)

    def loadStakes(self, minimum_validator_version):
        self.stakes = TransformData.loadStakes(minimum_validator_version)

    def mergeMoversWithStakes(self):
        self.df = TransformData.mergeDataframes(self.movers_df, self.stakes)

    def getPercentileDf(self):
        self.df = TransformData.getDataframePercentile(self.df, self.bottom_percentile, self.top_percentile)

    def initPlot(self):
        self.plot = Plotter(self.df, self.data_type_movers, (self.bottom_percentile, self.top_percentile))