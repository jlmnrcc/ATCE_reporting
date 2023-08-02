import pandas as pd
import os
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
folder = "..\\data\\2023w17"
df=readATCEextracts(folder)


#Splitting column names
column_names = df[:0].columns.values.tolist()
border_name=[]
value_name=[]
for i in column_names:
    border_name.append(i[0])
    value_name.append(i[1])

   
#List with border zones
border_name2 = []
[border_name2.append(item) for item in border_name[3:251] if item not in border_name2]

#Getting time and backup
time=pd.DataFrame(df.iloc[:,[0,1,2]].values, columns = ['MTU','CET','Backup'])

#Adding empty rows so it matches the values length for each hour
new_index = pd.RangeIndex(len(time)*(3))
df2 = pd.DataFrame(np.nan, index=new_index, columns=time.columns)
ids = np.arange(len(time))*(3)
df2.loc[ids] = time.values
df2=df2.set_index(['MTU','CET','Backup'])
    
data=df.drop(column_names[0:3],axis=1)

#Splitting every column into seperate dataframes
data_split=[]
[data_split.append(pd.DataFrame(data.iloc[:,i])) for i in range(data.shape[1]-2)]

#Find columns with same borders
border_id=[]
for i in range(data.shape[1]-3):
    if data_split[i].columns.values[0][0]!=data_split[i+1].columns.values[0][0]:
        border_id.append(i+1)
border_id.append(len(data_split))

#Join dataframes with same borders
border_list=[]

for i in range(len(border_id)):
    if i==0:
        border_list.append(data_split[0:border_id[i]])
    else:
        border_list.append(data_split[border_id[i-1]:border_id[i]])
        
#Concat dataframes and drop values NTC_initial
conc_values=[]
for i in range(len(border_list)):
    conc_values.append(pd.concat(border_list[i],axis=1))
    if conc_values[i].shape[1]==4:
        conc_values[i].drop(conc_values[i].columns[0], axis=1, inplace=True)


#reshape dataframes in order for them to fit the index made earlier
reshape=[]
[reshape.append(conc_values[i].iloc[:,:conc_values[i].shape[1]].values.reshape(conc_values[i].shape[1]*conc_values[i].shape[0],1)) for i in range(len(conc_values))]

#Concatenate all dataframes
total=np.concatenate(reshape, axis=1)

#Index list with value names
size=int(total.shape[0]/3)
new_valueList=[]
for i in range(size):
    new_valueList.append(value_name[4:7])
new_valueList=[j for i in new_valueList for j in i]

#Setting time and values names as index for the values in correct order
result = pd.DataFrame(total, index=list(new_valueList), columns=border_name2).set_index(df2.index,append=True).reorder_levels(order=[1,2,3,0])
result.to_csv("..\\data\\2023w17\\2023w17_CNTCParameters_public.csv")
