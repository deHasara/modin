import numpy as np
import pytest
import pandas

import modin.pandas as pd

pd.DEFAULT_NPARTITIONS = 4

customer=pd.DataFrame({
    'id':[1,2,3,4,5,6,7,8,9],
    'age':[20,25,15,10,30,65,35,18,23],
    'Product_ID':[101,0,106,0,103,104,0,0,107],
    'test_column':[1,2,3,4,5,6,7,8,9],
})

product=pd.DataFrame({
    'Product_ID':[101,102,103,104,105,106,107],
    'Price':[299.0,1350.50,2999.0,14999.0,145.0,110.0,79999.0],
    'test_column':[1,2,3,4,5,6,7],
})

#merge
#left -> customer
#result = pd.merge(customer, product, sort= True,on=['Product_ID']) #,'test_column'
result = pd.merge(customer, product, sort= False, on=[2, 0]) #default -> inner
#print('result: ')
#print(type(result))
#print(result)