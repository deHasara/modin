import pandas
from modin.engines.pyCylon.pandas_on_pyCylon.frame.pyCylon_table_manager import PandasOnPyCylonManager
from modin.backends.pandas.query_compiler import PandasQueryCompiler

class PandasOnPyCylonFrame(object):

    _frame_mgr_cls = PandasOnPyCylonManager
    _query_compiler_cls = PandasQueryCompiler

    @property
    def __constructor__(self):
        """The constructor for this object. A convenience method"""
        return type(self)

    def __init__(
            self,
            dataframe, #in modin this is partitions
            index,
            columns,
            row_lengths = None,
            column_lengths = None,
            dtypes = None,):
        self =self
        self.dataframe=dataframe
        self.index = index
        self.columns = columns
        self.dtypes = dtypes

    @property
    def axes(self):
        """The index, columns that can be accessed with an `axis` integer."""
        return [self.index, self.columns]

    @classmethod
    def from_pandas(cls, df):  # this is for initial pd.Dataframe() creation
        """Improve simple Pandas DataFrame to a PyCylon table (earlier it was from Pandas to Modin frame, now we don't have partition kinda thing)

        Args:
            df: Pandas DataFrame object.

        Returns:
            A new dataframe.
        """
        new_index = df.index
        new_columns = df.columns
        new_dtypes = df.dtypes
        # new_frame, new_lengths, new_widths = cls._frame_mgr_cls.from_pandas(df)  #PandasOnPyCylonFrame.PandasOnPyCylonManager().from_pandas(df)
        new_frame = cls._frame_mgr_cls.from_pandas(cls._frame_mgr_cls, df)
        return cls(new_frame,
                   new_index,
                   new_columns,
                   dtypes=new_dtypes,
                   )

    def to_pandas(self):  # to view dataframe e.g. print(customer)
        df = self._frame_mgr_cls.to_pandas(self._frame_mgr_cls, self.dataframe)

        if df.empty:
            if len(self.columns) != 0:
                df = pandas.DataFrame(columns=self.columns)
            else:
                df = pandas.DataFrame(columns=self.columns, index=self.index)
        '''
        else:
            ErrorMessage.catch_bugs_and_request_email(
                not df.index.equals(self.index) or not df.columns.equals(self.columns),
                "Internal and external indices do not match.",
            )
            #df.index = self.index
            #df.columns = self.columns
        '''
        return df

    def copy(self):
        """Copy this object.

        Returns:
            A copied version of this object.
        """
        return self.__constructor__(
            self.dataframe,
            self.index.copy(),
            self.columns.copy(),
            # self._row_lengths,  #we might not need this row lengths and column lengths in dataframe
            # self._column_widths,
            self.dtypes,
        )

    # Got only partial implementation from original BasePandasFrame class

    def mask(  # to view dataframe e.g. print(customer)
            self,
            row_indices=None,
            row_numeric_idx=None,
            col_indices=None,
            col_numeric_idx=None,
    ):
        """Lazily select columns or rows from given indices.

        Note: If both row_indices and row_numeric_idx are set, row_indices will be used.
            The same rule applied to col_indices and col_numeric_idx.

        Parameters
        ----------
        row_indices : list of hashable
            The row labels to extract.
        row_numeric_idx : list of int
            The row indices to extract.
        col_indices : list of hashable
            The column labels to extract.
        col_numeric_idx : list of int
            The column indices to extract.

        Returns
        -------
        BasePandasFrame
             A new BasePandasFrame from the mask provided.
        """
        if isinstance(row_numeric_idx, slice) and (
                row_numeric_idx == slice(None) or row_numeric_idx == slice(0, None)
        ):
            row_numeric_idx = None
        if isinstance(col_numeric_idx, slice) and (
                col_numeric_idx == slice(None) or col_numeric_idx == slice(0, None)
        ):
            col_numeric_idx = None
        if (
                row_indices is None
                and row_numeric_idx is None
                and col_indices is None
                and col_numeric_idx is None
        ):
            return self.copy()

    def merge(self, right, **kwargs):
        # Need a way to get new_index,new_columns,dtypes=new_dtypes for merged table
        # currently cylon table has no such thing
        # we can convert this cylon to pandas frame and get those values for the time being

        # in original BasePandasFrame class they got those by this -> check line 1227-1240
        new_frame = self._frame_mgr_cls.merge(self.dataframe, right.dataframe, **kwargs)
        self.dataframe = new_frame

        df = self.to_pandas()
        new_index = df.index
        new_columns = df.columns
        new_dtypes = df.dtypes

        return self.__constructor__(
            new_frame,
            new_index,
            new_columns,
            new_dtypes,

        )

    def _join_index_objects(self, axis, other_index, how, sort):
        """
        Joins a pair of index objects (columns or rows) by a given strategy.

        Unlike Index.join() in Pandas, if axis is 1, the sort is
        False, and how is "outer", the result will _not_ be sorted.

        Parameters
        ----------
            axis : 0 or 1
                The axis index object to join (0 - rows, 1 - columns).
            other_index : Index
                The other_index to join on.
            how : {'left', 'right', 'inner', 'outer'}
                The type of join to join to make.
            sort : boolean
                Whether or not to sort the joined index

        Returns
        -------
        Index
            Joined indices.
        """

        def merge_index(obj1, obj2):
            if axis == 1 and how == "outer" and not sort:
                return obj1.union(obj2, sort=False)
            else:
                # print(type(obj1))
                # print('obj1:', obj1.join(obj2, how=how, sort=sort))
                return obj1.join(obj2, how=how, sort=sort)

        if isinstance(other_index, list):
            joined_obj = self.columns if axis else self.index
            # TODO: revisit for performance
            for obj in other_index:
                joined_obj = merge_index(joined_obj, obj)
            return joined_obj
        if axis:
            return merge_index(self.columns, other_index)
        else:
            return self.index.join(other_index, how=how, sort=sort)

    def _copartition(self, axis, other, how, sort,
                     force_repartition=False):  # we need this partially to get joined index for cylon
        """
        Copartition two dataframes.

        Parameters
        ----------
            axis : 0 or 1
                The axis to copartition along (0 - rows, 1 - columns).
            other : BasePandasFrame
                The other dataframes(s) to copartition against.
            how : str
                How to manage joining the index object ("left", "right", etc.)
            sort : boolean
                Whether or not to sort the joined index.
            force_repartition : boolean
                Whether or not to force the repartitioning. By default,
                this method will skip repartitioning if it is possible. This is because
                reindexing is extremely inefficient. Because this method is used to
                `join` or `append`, it is vital that the internal indices match.

        Returns
        -------
        Tuple
            A tuple (left data, right data list, joined index).
        """
        if isinstance(other, type(self)):
            other = [other]

        index_other_obj = [o.axes[axis] for o in other]
        joined_index = self._join_index_objects(axis, index_other_obj, how, sort)

        return joined_index

    def _concat(self, axis, others, how, sort):
        """Concatenate this dataframe with one or more others.

        Args:
            axis: The axis to concatenate over.
            others: The list of dataframes to concatenate with.
            how: The type of join to use for the axis.
            sort: Whether or not to sort the result.

        Returns:
            A new dataframe.
        """
        # Fast path for equivalent columns and partitioning
        if (
                axis == 0
                and all(o.columns.equals(self.columns) for o in others)
                # and all(o._column_widths == self._column_widths for o in others)  #we don't need column widths or row lengths for cylon
        ):
            # print('axis is 0')
            joined_index = self.columns  # joined_index means column names for new dataframe
            # left_parts = self.dataframe
            right_parts = [o.dataframe for o in others]
            new_lengths = None
            new_widths = None

        elif (
                axis == 1
                and all(o.index.equals(self.index) for o in others)
                # and all(o._row_lengths == self._row_lengths for o in others)
        ):
            joined_index = self.index  # since axis=1 joined_index means index of any dataframe (since index is equal in all dataframes)
            # left_parts = self.dataframe
            right_parts = [o.dataframe for o in others]  # o.dataframe is a cylontable type
            new_lengths = None
            new_widths = None

        # unequivaluent columns in tables to concat
        else:
            # print('self partitions: ', self._partitions)
            '''
            left_parts, right_parts, joined_index = self._copartition(
                axis ^ 1, others, how, sort, force_repartition=True
            )
            '''
            # left_parts = self.dataframe
            right_parts = [o.dataframe for o in others]
            joined_index = self._copartition(
                axis ^ 1, others, how, sort, force_repartition=True
            )
            new_lengths = None
            new_widths = None

        new_frame = self._frame_mgr_cls.concat(self.dataframe, right_parts, axis)
        if axis == 0:
            new_index = self.index.append([other.index for other in others])
            new_columns = joined_index
            # TODO: Can optimize by combining if all dtypes are materialized
            new_dtypes = None
        else:
            new_columns = self.columns.append([other.columns for other in others])
            new_index = joined_index
            if self._dtypes is not None and all(o._dtypes is not None for o in others):
                new_dtypes = self.dtypes.append([o.dtypes for o in others])
            else:
                new_dtypes = None
        return self.__constructor__(
            new_frame, new_index, new_columns, new_lengths, new_widths, new_dtypes
        )


