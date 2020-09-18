from modin.engines.base.io import BaseIO
from modin.backends.pandas.query_compiler import PandasQueryCompiler
from modin.engines.pyCylon.pandas_on_pyCylon.frame.data import PandasOnPyCylonFrame


class PandasOnPyCylonIO(BaseIO):

    frame_cls = PandasOnPyCylonFrame
    query_compiler_cls = PandasQueryCompiler
