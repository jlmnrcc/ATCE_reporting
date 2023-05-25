#!/usr/bin/python
'''
'''
import numpy as np
import scipy.sparse as sp
import requests
import pandas as pd
import datetime
import pytz
import topology
import atce_graphical_report

def get_fb_data(start_time, end_time):

    url = "https://test-publicationtool.jao.eu/nordic/api/data/finalComputation"
    header = {"Authorization": "Bearer ***token*** (will be implemented in future release)"}
    query = {"filter":[],
            "skip":0,
            "take":"",
            "fromUtc": "{:04}-{:02}-{:02}T{:02}:00:00.000Z".format(start_time.year, start_time.month,start_time.day ,start_time.hour),
            "toUtc": "{:04}-{:02}-{:02}T{:02}:00:00.000Z".format(end_time.year, end_time.month, end_time.day, end_time.hour)}
             

    r = requests.get(url, headers=header, data=query)
    
    if r.status_code >= 200 and r.status_code < 300:
        j = r.json()
        df = pd.DataFrame.from_dict(j['data'])
    else:
        HTMLResponseError = "HTML query returned an unexpected respone. Status_code: " + str(r.status_code)
        raise Exception(HTMLResponseError)
    
    df["dateTimeUtc"] = pd.to_datetime(df["dateTimeUtc"], format="%Y-%m-%dT%H:%M:00Z", utc=True)
    df = df.sort_values(by='dateTimeUtc',ascending=True)
    
    return df


def dropTolerance(B,tol):
    A=B.copy()
    mask = list(A.data<tol)
    A.data[mask]=0
    return A


if __name__=="__main__":
    
    path = "..\\data\\ATCEvalidationToolTestData"
    
    bzbs = [b for b in topology.latest_topology["borders"] if b['inFbTopology']]
    
    print("Importing ATCE results...")
    atcdf = atce_graphical_report.readATCEextracts(path)
    print("... done!")
    
    start = atcdf["dateTime"].min()
    end = atcdf["dateTime"].max() + datetime.timedelta(hours=1)
    
    print('Querying FB data from JAO...')
    fbdf = get_fb_data(start, end)
    fbdf.to_csv(path + '\\' + 'jao_data.csv')
    # fbdf = pd.read_csv( path + '\\' + "jao_data.csv")
    fbdf["dateTimeUtc"] = pd.to_datetime(fbdf["dateTimeUtc"], format="%Y-%m-%d %H:%M:00+00:00", utc=True)
    
    fbdf = fbdf.sort_values(by='dateTimeUtc',ascending=True)
    
    print('... done!')
    
    atce_bzb_order = []
    fb_bzb_order = []
    
    for brb in bzbs:
        fb_bzb_order.append('z2z_ptdf_' + brb['name'])
        fbdf["z2z_ptdf_"+brb['name']]=fbdf["ptdf_"+brb['from']]-fbdf["ptdf_"+brb['to']]
        
        if len(brb['mappedNTCborder'])>0:
            atce_name = brb['mappedNTCborder'][0]
        else:
            atce_name = brb['name']
        atce_bzb_order.append(atce_name) 
        if not atce_name in atcdf:
            print("Warning! Bidding zone: " + atce_name + " was not found in ATCE result data. Missing values are treated as 0")
            atcdf[atce_name, 'AAC'] = [0] * len(atcdf["dateTime"])
            atcdf[atce_name, 'NTC_final'] = [0] * len(atcdf["dateTime"])
            
          
    dropTol = 0.05
    
    f_aac0 = np.array([0.0]*len(fbdf['id']))
    f_aac_tol = np.array([0]*len(fbdf['id']))
    AAC_ntc_0 = np.array([0]*len(fbdf['id']))
    AAC_ntc_tol = np.array([0]*len(fbdf['id']))
    
    print('Calculating CNEC flows...')
    
    A = sp.csc_matrix(fbdf.loc[:, fb_bzb_order].values, shape=(len(fbdf[fb_bzb_order[0]]),len(fb_bzb_order)))
    A_tol = dropTolerance(A, dropTol)
    A_zer = dropTolerance(A, 1e-16)
    A_tol = A_tol.transpose()
    A_zer = A_zer.transpose()
    
    for mtu in atcdf['dateTime']:
        print('... ' + mtu.strftime('%Y-%m-%d %H:%M') + ' ...')
        cnec_idx = [ii for ii in fbdf.index if fbdf["dateTimeUtc"][ii]==mtu]#todo
        aacs = np.array([atcdf.loc[atcdf['dateTime']==mtu, (bzb, 'AAC')].values[0] for bzb in atce_bzb_order])
        ntcs = np.array([atcdf.loc[atcdf['dateTime']==mtu, (bzb, 'NTC_final')].values[0] for bzb in atce_bzb_order])
        
        f_aac0[cnec_idx] = aacs * A_zer[:,cnec_idx]
        f_aac_tol[cnec_idx] = aacs * A_tol[:,cnec_idx]
        AAC_ntc_0[cnec_idx] = ntcs * A_zer[:,cnec_idx]
        AAC_ntc_tol[cnec_idx] = ntcs * A_tol[:,cnec_idx]
        
    
    fbdf['f_aac0'] = f_aac0
    fbdf['f_aac_tol'] = f_aac_tol
    fbdf['AAC_ntc_0'] = AAC_ntc_0
    fbdf['AAC_ntc_tol'] = AAC_ntc_tol
    
    fbdf['f_aac0_ratio'] = fbdf['f_aac0'] / fbdf['ram']
    fbdf['f_aac_tol_ratio'] = fbdf['f_aac_tol'] / fbdf['ram']
    fbdf['AAC_ntc_0_ratio'] = fbdf['AAC_ntc_0'] / fbdf['ram']
    fbdf['AAC_ntc_tol_ratio'] = fbdf['AAC_ntc_tol'] / fbdf['ram']
 
    print('Writing output...')

    fbdf.to_csv(path + "\\atce_validation_prototype.csv")
    print('done!')
    
    
