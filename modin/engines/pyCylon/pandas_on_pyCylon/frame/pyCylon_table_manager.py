import numpy as np
import pandas
from pycylon.data.table import Table
import pyarrow as pa

from pycylon.ctx.context import CylonContext
import argparse


ctx: CylonContext = CylonContext("mpi")

#look at modin/engines/base/frame/partition_manager.py

#This is the BaseFrameManager for pyCylon
class PandasOnPyCylonManager(object):

    def from_pandas(cls, df):
        # convert pandas to cylon table object -> df = cylon.Table(df)
        arw_table = pa.Table.from_pandas(df)
        cylon_table = Table.from_arrow(arw_table)
        return cylon_table

    def to_pandas(cls, cylon_table): # to view dataframe e.g. print(customer)
        arw_table = Table.to_arrow(cylon_table)
        df = arw_table.to_pandas()
        return df

    def merge(self, right, **kwargs):
        join_type = kwargs.get("how", "inner")
        on = kwargs.get("on", None)
        # call pycylon.merge(self, right)
        tb1: Table = self.join(ctx, table=right, join_type=join_type, algorithm='hash', left_col=on[0], right_col=on[1]) #left_col=3
        return tb1

    def concat(self, right_table_list, axis): #(self, right, axis)
        #here can pass list of tables
        tb1: Table = self.distributed_union(ctx, table=right_table_list[0]) #since we are concatenating self with only one -> right_table_list[0]
        return tb1



