# -*- coding: utf-8 -*-
"""
Created on Thu May 25 10:27:33 2023

@author: NoaHvidShiv
"""
import os
from shutil import which
import pandas as pd
from entsoe import EntsoePandasClient as Entsoe
import datetime
import numpy as np



def readATCEextracts(folder):
    #Return current entries in path
    files = os.listdir(os.getcwd() + "\\" + folder)
    files = [f for f in files if "extract.csv" in f]   
    
    #Read file, if multiple then concat
    df = pd.read_csv(folder + "\\" + files[0], header=[0, 1])
    if len(files)>1:
        for file in files[1::]:
            df = pd.concat([df, pd.read_csv(folder + "\\" + file, header=[0, 1])])
    
    df["dateTime"] = pd.to_datetime(df['MTU']["MTU"], format="%Y-%m-%dT%H:%MZ", utc=True)
    
    return df
folder = "..\\data\\2023w18"
df=readATCEextracts(folder)


column_names = df[:0].columns.values.tolist()
c_name=[]
i_name=[]
for i in column_names:
    c_name.append(i[0])
    i_name.append(i[1])

for i in range(3):
    i_name[i]=column_names[i]
   

new_list = []
[new_list.append(item) for item in c_name[3:251] if item not in new_list]

mypivot = pd.pivot_table(df, values=c_name[3:253], index=column_names[0:3])
time=mypivot.index

#time=time.reset_index(inplace=True, level=['MTU','CET','Backup'])

MTU=[]
CET=[]
Backup=[]
for i in range(len(mypivot)):
    MTU.append(mypivot.index[i][0])
    CET.append(mypivot.index[i][1])
    Backup.append(mypivot.index[i][2])
MTU=np.array(MTU)
CET=np.array(CET)
Backup=np.array(Backup)
time= np.stack([MTU,CET,Backup],axis=1)
time2 = pd.DataFrame(time, columns = ['MTU','CET','Backup'])
#time2=time2.set_index(['MTU','CET','Backup'])


n = 3
new_index = pd.RangeIndex(len(time2)*(n+1))
df2 = pd.DataFrame(np.nan, index=new_index, columns=time2.columns)
ids = np.arange(len(time))*(n+1)
df2.loc[ids] = time2.values
df2=df2.set_index(['MTU','CET','Backup'])
    
data=df.drop(column_names[0:3],axis=1)


data2=[]
for i in range((data.shape[1])-2):
    if data.columns.values[i][0]==c_name[i+3]:
        data2.append(data.iloc[:,i:i+1])

data3=[]
data4=[]
for i in range(data.shape[1]-3):
    if data2[i].columns.values[0][0]!=data2[i+1].columns.values[0][0]:
        data3.append(i+1)
data3.append(len(data2))

for i in range(len(data3)):
    if i==0:
        data4.append(data2[0:data3[i]])
    else:
        data4.append(data2[data3[i-1]:data3[i]])

data5=[]
for i in range(len(data4)):
    data5.append(pd.concat(data4[i],axis=1))
    if data5[i].shape[1]==3:
        data5[i].insert(0,"NTC_initial", " ")


shape=[]
samlet=[]
for i in range(len(data5)):
    shape.append(data5[i].iloc[:,:data5[i].shape[1]].values.reshape(data5[i].shape[1]*data5[i].shape[0],1))

samlet=np.concatenate(shape, axis=1)

ind=[]
for i in range( 168):
    ind.append(i_name[3:4])
    ind.append(i_name[4:5])
    ind.append(i_name[5:6])
    ind.append(i_name[6:7])



ind=[j for i in ind for j in i]
res = pd.DataFrame(samlet, index=list(ind), columns=new_list) 
result =res.set_index(df2.index,append=True)
result=result.reorder_levels(order=[1,2,3,0])

result.to_csv("output.csv")