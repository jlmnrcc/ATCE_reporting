#!/usr/bin/python
'''
'''
import numpy as np
import scipy.sparse as sp
import requests
import pandas as pd
import datetime
import pytz
import atce_grephical_report

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
    bzbs = [{'from':"DK1", 'to':"DK1_CO", 'borderDirection':"DK1-DK1_CO", "ig107name": "DK1-DK1_CO"},
            {'from':"DK1", 'to':"DK1_DE", 'borderDirection':"DK1-DK1_DE", "ig107name": "DK1-DK1_DE"},
            {'from':"DK1", 'to':"DK1_KS", 'borderDirection':"DK1-DK1_KS", "ig107name": "DK1A-SE3"},
            {'from':"DK1", 'to':"DK1_SK", 'borderDirection':"DK1-DK1_SK", "ig107name": "DK1-DK1_SK"},
            {'from':"DK1", 'to':"DK1_SB", 'borderDirection':"DK1-DK1_SB", "ig107name": "DK1-DK2"},
            {'from':"DK1_CO", 'to':"DK1", 'borderDirection':"DK1_CO-DK1", "ig107name": "DK1_CO-DK1"},
            {'from':"DK1_DE", 'to':"DK1", 'borderDirection':"DK1_DE-DK1", "ig107name": "DK1_DE-DK1"},
            {'from':"DK1_KS", 'to':"DK1", 'borderDirection':"DK1_KS-DK1", "ig107name": "SE3-DK1A"},
            {'from':"DK1_SK", 'to':"DK1", 'borderDirection':"DK1_SK-DK1", "ig107name": "NO2-DK1A"},
            {'from':"DK1_SB", 'to':"DK1", 'borderDirection':"DK1_SB-DK1" ,"ig107name": "DK2-DK1"},
            {'from':"DK2", 'to':"DK2_KO", 'borderDirection':"DK2-DK2_KO" ,"ig107name": "DK2-DK2_KO"},
            {'from':"DK2", 'to':"DK2_SB", 'borderDirection':"DK2-DK2_SB" ,"ig107name": "DK2-DK1"},
            {'from':"DK2", 'to':"SE4", 'borderDirection':"DK2-SE4", "ig107name": "DK2-SE4"},
            {'from':"DK2_KO", 'to':"DK2", 'borderDirection':"DK2_KO-DK2", "ig107name": "DK2_KO-DK2"},
            {'from':"DK2_SB", 'to':"DK2", 'borderDirection':"DK2_SB-DK2", "ig107name": "DK1-DK2"},
            {'from':"FI", 'to':"FI_EL", 'borderDirection':"FI-FI_EL", "ig107name": "FI-FI_EL"},
            {'from':"FI", 'to':"FI_FS", 'borderDirection':"FI-FI_FS", "ig107name": "FI-SE3"},
            {'from':"FI", 'to':"NO4", 'borderDirection':"FI-NO4", "ig107name": "FI-NO4"},
            {'from':"FI", 'to':"SE1", 'borderDirection':"FI-SE1", "ig107name": "FI-SE1"},
            {'from':"FI_EL", 'to':"FI", 'borderDirection':"FI_EL-FI", "ig107name": "FI_EL-FI"},
            {'from':"FI_FS", 'to':"FI", 'borderDirection':"FI_FS-FI", "ig107name": "SE3-FI"},
            {'from':"NO1", 'to':"NO2", 'borderDirection':"NO1-NO2", "ig107name": "NO1A-NO2"},
            {'from':"NO1", 'to':"NO3", 'borderDirection':"NO1-NO3", "ig107name": "NO1-NO3"},
            {'from':"NO1", 'to':"NO5", 'borderDirection':"NO1-NO5", "ig107name": "NO1A-NO5"},
            {'from':"NO1", 'to':"SE3", 'borderDirection':"NO1-SE3", "ig107name": "NO1-SE3"},
            {'from':"NO2", 'to':"NO1", 'borderDirection':"NO2-NO1", "ig107name": "NO2-NO1A"},
            {'from':"NO2", 'to':"NO2_ND", 'borderDirection':"NO2-NO2_ND", "ig107name": "NO2-NO2_ND"},
            {'from':"NO2", 'to':"NO2_NK", 'borderDirection':"NO2-NO2_NK", "ig107name": "NO2-NO2_NK"},
            {'from':"NO2", 'to':"NO2_SK", 'borderDirection':"NO2-NO2_SK", "ig107name": "NO2-NO2_SK"},
            {'from':"NO2", 'to':"NO5", 'borderDirection':"NO2-NO5", "ig107name": "NO2-NO5"},
            {'from':"NO2_ND", 'to':"NO2", 'borderDirection':"NO2_ND-NO2", "ig107name": "NO2_ND-NO2"},
            {'from':"NO2_NK", 'to':"NO2", 'borderDirection':"NO2_NK-NO2", "ig107name": "NO2_NK-NO2"},
            {'from':"NO2_SK", 'to':"NO2", 'borderDirection':"NO2_SK-NO2", "ig107name": "DK1A-NO2"},
            {'from':"NO3", 'to':"NO1", 'borderDirection':"NO3-NO1", "ig107name": "NO3-NO1"},
            {'from':"NO3", 'to':"NO4", 'borderDirection':"NO3-NO4", "ig107name": "NO3-NO4"},
            {'from':"NO3", 'to':"NO5", 'borderDirection':"NO3-NO5", "ig107name": "NO3-NO5"},
            {'from':"NO3", 'to':"SE2", 'borderDirection':"NO3-SE2", "ig107name": "NO3-SE2"},
            {'from':"NO4", 'to':"FI", 'borderDirection':"NO4-FI", "ig107name": "NO4-FI"},
            {'from':"NO4", 'to':"NO3", 'borderDirection':"NO4-NO3", "ig107name": "NO4-NO3"},
            {'from':"NO4", 'to':"SE1", 'borderDirection':"NO4-SE1", "ig107name": "NO4-SE1"},
            {'from':"NO4", 'to':"SE2", 'borderDirection':"NO4-SE2", "ig107name": "NO4-SE2"},
            {'from':"NO5", 'to':"NO1", 'borderDirection':"NO5-NO1", "ig107name": "NO5-NO1A"},
            {'from':"NO5", 'to':"NO2", 'borderDirection':"NO5-NO2", "ig107name": "NO5-NO2"},
            {'from':"NO5", 'to':"NO3", 'borderDirection':"NO5-NO3", "ig107name": "NO5-NO3"},
            {'from':"SE1", 'to':"FI", 'borderDirection':"SE1-FI", "ig107name": "SE1-FI"},
            {'from':"SE1", 'to':"NO4", 'borderDirection':"SE1-NO4", "ig107name": "SE1-NO4"},
            {'from':"SE1", 'to':"SE2", 'borderDirection':"SE1-SE2", "ig107name": "SE1-SE2"},
            {'from':"SE2", 'to':"NO3", 'borderDirection':"SE2-NO3", "ig107name": "SE2-NO3"},
            {'from':"SE2", 'to':"NO4", 'borderDirection':"SE2-NO4", "ig107name": "SE2-NO4"},
            {'from':"SE2", 'to':"SE1", 'borderDirection':"SE2-SE1", "ig107name": "SE2-SE1"},
            {'from':"SE2", 'to':"SE3", 'borderDirection':"SE2-SE3", "ig107name": "SE2-SE3"},
            {'from':"SE3", 'to':"NO1", 'borderDirection':"SE3-NO1", "ig107name": "SE3-NO1"},
            {'from':"SE3", 'to':"SE2", 'borderDirection':"SE3-SE2", "ig107name": "SE3-SE2"},
            {'from':"SE3", 'to':"SE3_FS", 'borderDirection':"SE3-SE3_FS", "ig107name": "SE3-FI"},
            {'from':"SE3", 'to':"SE3_KS", 'borderDirection':"SE3-SE3_KS", "ig107name": "SE3-DK1A"},
            {'from':"SE3", 'to':"SE3_SWL", 'borderDirection':"SE3-SE3_SWL", "ig107name": "SE3-SE4-DC"},
            {'from':"SE3", 'to':"SE4", 'borderDirection':"SE3-SE4", "ig107name": "SE3-SE4"},
            {'from':"SE3_FS", 'to':"SE3", 'borderDirection':"SE3_FS-SE3", "ig107name": "FI-SE3"},
            {'from':"SE3_KS", 'to':"SE3", 'borderDirection':"SE3_KS-SE3", "ig107name": "DK1A-SE3"},
            {'from':"SE3_SWL", 'to':"SE3", 'borderDirection':"SE3_SWL-SE3", "ig107name": "SE3_SWL-SE3"},
            {'from':"SE4", 'to':"DK2", 'borderDirection':"SE4-DK2", "ig107name": "SE4-DK2"},
            {'from':"SE4", 'to':"SE3", 'borderDirection':"SE4-SE3", "ig107name": "SE4-SE3"},
            {'from':"SE4", 'to':"SE4_BC", 'borderDirection':"SE4-SE4_BC", "ig107name": "SE4-SE4_BC"},
            {'from':"SE4", 'to':"SE4_NB", 'borderDirection':"SE4-SE4_NB", "ig107name": "SE4-SE4_NB"},
            {'from':"SE4", 'to':"SE4_SP", 'borderDirection':"SE4-SE4_SP", "ig107name": "SE4-SE4_SP"},
            {'from':"SE4", 'to':"SE4_SWL", 'borderDirection':"SE4-SE4_SWL", "ig107name": "SE4-SE4_SWL"},
            {'from':"SE4_BC", 'to':"SE4", 'borderDirection':"SE4_BC-SE4", "ig107name": "SE4_BC-SE4"},
            {'from':"SE4_NB", 'to':"SE4", 'borderDirection':"SE4_NB-SE4", "ig107name": "SE4_NB-SE4"},
            {'from':"SE4_SP", 'to':"SE4", 'borderDirection':"SE4_SP-SE4", "ig107name": "SE4_SP-SE4"},
            {'from':"SE4_SWL", 'to':"SE4", 'borderDirection':"SE4_SWL-SE4", "ig107name": "SE4_SWL-SE4"}]

    print("Importing ATCE results...")
    atcdf = atce_grephical_report.readATCEextracts("..\\data\\smaller_tolerance")
    print("... done!")
    
    start = atcdf["dateTime"].min()
    end = atcdf["dateTime"].max() + datetime.timedelta(hours=1)
    
    print('Querying FB data from JAO...')
    fbdf = get_fb_data(start, end)
    fbdf.to_csv('jao_data.csv')
    # fbdf = pd.read_csv("jao_data.csv")
    # fbdf["dateTimeUtc"] = pd.to_datetime(fbdf["dateTimeUtc"], format="%Y-%m-%d %H:%M:00+00:00", utc=True)
    
    fbdf = fbdf.sort_values(by='dateTimeUtc',ascending=True)
    
    print('... done!')
    
    atce_bzb_order = []
    fb_bzb_order = []
    
    for brb in bzbs:
        fb_bzb_order.append('z2z_ptdf_' + brb['borderDirection'])
        atce_bzb_order.append(brb['ig107name'])
        fbdf["z2z_ptdf_"+brb['borderDirection']]=fbdf["ptdf_"+brb['from']]-fbdf["ptdf_"+brb['to']]
        if not brb['ig107name'] in atcdf:
            print("Warning! Bidding zone: " + brb['borderDirection'] + " (" + brb['ig107name'] + ") was not found in ATCE result data. Missing values are treated as 0")
            atcdf[brb['ig107name'], 'AAC'] = [0] * len(atcdf["dateTime"])
            atcdf[brb['ig107name'], 'NTC_final'] = [0] * len(atcdf["dateTime"])
    
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

    fbdf.to_csv("out.csv")
    print('done!')
    
    
