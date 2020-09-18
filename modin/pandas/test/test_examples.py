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
print(result)

'''
customer2 = pd.DataFrame({
    'id':[1,2,3,4,5,6,7,10,11,12,13,14,15,16,17],
    'age':[20,25,12,10,30,12,45,12,34,12,14,57,78,35,45],
    'Product_ID':[101,0,106,0,103,104,0,108,109,111,112,114,111,110,111],
    'test_column':[1,2,3,4,5,6,7,10,11,13,14,15,16,3,4],
})
customer3 = pd.DataFrame({
    'id':[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17],
    'age':[20,25,12,10,30,12,45,12,34,12,14,57,78,35,45,89,67],
    'Product_ID':[101,0,106,0,103,104,0,108,109,111,112,114,111,110,111,110,112],
    'test_column':[1,2,3,4,5,6,7,10,11,13,14,15,16,3,4,15,16],
})

'''
#concat
#modin_concat = pd.concat([customer2, customer3], join='inner', sort=False, axis=0) #join type inner or outer #axis =0/1 0 means concat on index, 1 is on columns
#print(modin_concat)
