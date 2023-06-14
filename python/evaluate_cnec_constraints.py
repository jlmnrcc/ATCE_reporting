#!/usr/bin/python
'''
'''
import numpy as np
import scipy.sparse as sp
from scipy.optimize import linprog
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


def appendNtcAac(fbdf, atcdf, bzbs, ptdfRelaxationTolerance):
    '''
    Input: 
        - fbdf (pandas dataframe containing flow-based parameters)
        - atcdf (pandas dataframe containing ATCE results)
        - bzbs (list of dicts from topology.latest_topology['borders'] including bidding zone borders in the flow-based market topology)
        - ptdfRelaxationThreshold (float representing the tolerance of z2zPTDFs which are ignored in ATC extraction)
    
    Calculates z2zPTDFs of bidding zone borders in the flow-based market topology
    Utilizes mapping between flow-based and NTC topology to relate extracted NTCs to flow-based borders
    Calculates the worst case allocated capacity on CNEC level (AAC_NTC) as the sum of z2zPTDF*NTC on all bidding zone borders
    AAC_NTC is calculated in 2 versions:
        - AAC_NTC_0 is the worst case loading, without ptdf relaxation being applied
        - AAC_NTC_tol is the loading including ptdf relaxation (i.e. the constrain used in ATC extraction)
    
    Calculates the ratio between RAM and AAC_NTC_0 and AAC_NTC_tol, respectively (a ratio greater than 1 indicates risk of violating the constraint)
    
    output:
        - fbdf ammended with AAC_NTCs and ratio to RAM
    '''
    
    atce_bzb_order = []
    fb_bzb_order = []
    border_CNECs = []
    
    for brb in bzbs:
        fb_bzb_order.append('z2z_ptdf_' + brb['name'])
        fbdf["z2z_ptdf_"+brb['name']]=fbdf["ptdf_"+brb['from']]-fbdf["ptdf_"+brb['to']]
        border_CNECs.append('border_CNEC_' + brb['name'])
        
        if len(brb['mappedNTCborder'])>0:
            atce_name = brb['mappedNTCborder'][0]
        else:
            atce_name = brb['name']

        atce_bzb_order.append(atce_name) 

        if not atce_name in atcdf:
            print("Warning! Bidding zone: " + atce_name + " was not found in ATCE result data. Missing values are treated as 0")
            atcdf[atce_name, 'AAC'] = [0] * len(atcdf["dateTime"])
            atcdf[atce_name, 'NTC_final'] = [0] * len(atcdf["dateTime"])
    
    AAC_ntc_0 = np.array([0]*len(fbdf['id']))
    AAC_ntc_tol = np.array([0]*len(fbdf['id']))
    
    print('Calculating CNEC flows...')
    
    A = sp.csc_matrix(fbdf.loc[:, fb_bzb_order].values, shape=(len(fbdf[fb_bzb_order[0]]),len(fb_bzb_order)))
    A_tol = dropTolerance(A, ptdfRelaxationTolerance)
    A_zer = dropTolerance(A, 1e-16)
    A_tol = A_tol.transpose()
    A_zer = A_zer.transpose()
    
    for mtu in atcdf['dateTime']:
        print('... ' + mtu.strftime('%Y-%m-%d %H:%M') + ' ...')
        cnec_idx = [ii for ii in fbdf.index if fbdf["dateTimeUtc"][ii]==mtu]
        aacs = np.array([atcdf.loc[atcdf['dateTime']==mtu, (bzb, 'AAC')].values[0] for bzb in atce_bzb_order])
        ntcs = np.array([atcdf.loc[atcdf['dateTime']==mtu, (bzb, 'NTC_final')].values[0] for bzb in atce_bzb_order])
        AAC_ntc_0[cnec_idx] = ntcs * A_zer[:,cnec_idx]
        AAC_ntc_tol[cnec_idx] = ntcs * A_tol[:,cnec_idx]
        
    fbdf['AAC_ntc_0'] = AAC_ntc_0
    fbdf['AAC_ntc_tol'] = AAC_ntc_tol
    fbdf['AAC_ntc_0_ratio'] = fbdf['AAC_ntc_0'] / fbdf['ram']
    fbdf['AAC_ntc_tol_ratio'] = fbdf['AAC_ntc_tol'] / fbdf['ram']
    
    print('... done!')
    
    return fbdf
 

def appendDAmarketCouplingFlows(npdf, fbdf):
    # Todo
    '''
    import DA market result (IG-113 or similar)
    Calculate market coupling flows per CNEC by zone-to-slack PTDF times net position
    append to fbdf dataframe and return
    '''
    
    dateTimeUtc = []
    for idx,row in npdf.iterrows():
        t = row['EDD']+ datetime.timedelta(hours=row['MTU']-1)
        # Todo: Localizing timestamp to UTC is not consistent with daylight saving time. This is because GC matrix does not provide a time-stamp - only an MTU number.
        dateTimeUtc.append(pytz.timezone('CET').localize(t).astimezone(pytz.utc))
    
    npdf['dateTimeUtc'] = dateTimeUtc
    print(npdf['dateTimeUtc'])
    
    bz_order = ['_'.join(c.split('_')[1::]) for c in fbdf.columns if c.split('_')[0]=='ptdf']
    ptdf_columns = [c for c in fbdf.columns if c.split('_')[0]=='ptdf']

    MTUs = fbdf['dateTimeUtc'].unique()
    fbdb = fbdf.sort_values(by=['dateTimeUtc'])
    
    z2sPtdfs = np.array(fbdf.loc[:, ptdf_columns])    
    MCR_flows = np.zeros((len(fbdf[ptdf_columns[0]])))
    
    print('Calculating MCR flows for CNECs...')
    for mtu in MTUs:
        print('MTU: ' + datetime.datetime.strftime(mtu, '%Y%m%d %H:%M') + '...')
        nps = np.array([npdf.loc[npdf['dateTimeUtc']==mtu, bz].values[0] for bz in bz_order])
        cnec_idx = [ii for ii in fbdf.index if fbdf["dateTimeUtc"][ii]==mtu]
        MCR_flows[cnec_idx] = np.inner(z2sPtdfs[cnec_idx,:],nps)
        
        
        
    fbdf['MCR_flows'] = MCR_flows
    fbdf['ID_ram'] = fbdf['ram'] - fbdf['MCR_flows']
    
    return fbdf

def appendMaximumIntradayFlows(fbdf, atcdf):
    '''
    For each CNEC, c, where AAC_NTC_0 is greater than RAM do:
        
        maxFlow <- maximize z2zPTDFc_T * Ex
        
        s.t.
            Ex(a->b) + Ex(b->a) = 0
            Ex(a->VBZa) - Ex(VBZb->b) = 0
            
        and lower/upper bounds:
            -1* NTC(b->a) < Ex(a->b) < NTC(a->b)
    '''

    borderOrder = [s.replace('z2z_ptdf_', '') for s in fbdf.columns if 'z2z_ptdf' in s]
    print(borderOrder)
    
    InternalHVDCsBorderDirections = [m for m in topology.latest_topology['borders'] if m['type']=='HvdcDifferentAreaNordicCCR' or m['type']=="HvdcSameAreaNordicCCR"]
    NumberOfInternalHVDCsBorders = len(InternalHVDCsBorderDirections)
        
    fb_borders=[]
    for b in borderOrder:
        fb_borders.append(next(iter([m for m in topology.latest_topology['borders'] if m['name']==b])))
    
    numberOfFbBorderDirections = len(fb_borders)
    
    
    # Create map from Exchange to bidding zone net position
    bzOrder = [n['norCapShortName'] for n in topology.latest_topology['biddingZones'] if n['inFbTopology']]
    borderToZoneMap = np.zeros((len(bzOrder), len(borderOrder)))
    
    for m,b in enumerate(borderOrder):
        bzb = next(iter([m for m in topology.latest_topology['borders'] if m['name']==b]))
        n = bzOrder.index(bzb['from'])
        borderToZoneMap[n,m] = 1
    
    
    # Set equality contstraints
    A_eq = np.zeros((numberOfFbBorderDirections + NumberOfInternalHVDCsBorders, numberOfFbBorderDirections))
    b_eq = np.zeros(numberOfFbBorderDirections + NumberOfInternalHVDCsBorders)
    print(A_eq.shape)

    for k,m in enumerate(fb_borders):
        A_eq[k,k] = 1
        A_eq[k, borderOrder.index(m['oppositeDirection'])] = 1
        
    for k,m in enumerate(InternalHVDCsBorderDirections):
        n_to = next(iter([n for n in topology.latest_topology['biddingZones'] if n['norCapShortName'] == m['to']]))
        if n_to['virtualBZ']:
            sending_end = m
            oppositeVBZ = n_to['oppositeVBZ']
            receiving_end = next(iter([x for x in fb_borders if x['from']== oppositeVBZ]))
            print(sending_end['name'] + " was identified as sending end to " + receiving_end['name'])
            A_eq[numberOfFbBorderDirections + k, borderOrder.index(sending_end['name'])] = 1
            A_eq[numberOfFbBorderDirections + k, borderOrder.index(receiving_end['name'])] = -1

    print(A_eq)
    
    # for each cnec: set up objective function, lower/upper bounds and run optimization
    IDmaxFlows=[]
    IDmaxFlowNPs = []
    for idx, cnec in fbdf.iterrows():
        if cnec['AAC_ntc_0'] > cnec['ram']:
            mtu = cnec['dateTimeUtc']
            print(mtu)
            print('  ')
            
            # set objective function c.transpose * x:
            c = -1*np.array([max(0.0, cnec[s]) for s in fbdf.columns if 'z2z_ptdf' in s])

            # Set lower upper bounds
            lb_ub = []
            for m in fb_borders:
                reverse_m = next(iter([r for r in fb_borders if r['name']==m['oppositeDirection']]))
                if m['inNtcTopology']:
                    maxNTC = atcdf.loc[atcdf['dateTime']==mtu, (m['name'], 'NTC_final')].values[0]
                    minNTC = -1 * atcdf.loc[atcdf['dateTime']==mtu, (reverse_m['name'], 'NTC_final')].values[0]
                else:
                    try:
                        maxNTC = atcdf.loc[atcdf['dateTime']==mtu, (m['mappedNTCborder'][0], 'NTC_final')].values[0]
                        minNTC = -1 * atcdf.loc[atcdf['dateTime']==mtu, (reverse_m['mappedNTCborder'][0], 'NTC_final')].values[0]
                    except IndexError:
                        print("Warning: Mapped NTC border of bzb " + m['name'] + ' not found. Assigning zero upper and lower bounds.')
                        maxNTC = 0
                        minNTC = 0
                    
                lb_ub.append((minNTC, maxNTC))
            
            # run optimization
            res = linprog(c, A_eq=A_eq, b_eq=b_eq, bounds=lb_ub)
            
            # Check result and append
            if res.fun is None:
                print(res.message)
                IDmaxFlows.append(99999)
                IDmaxFlowNPs.append(None)
            else:
                NPs = np.matmul(borderToZoneMap, res.x)
                if sum(NPs)**2 > 1e-12:
                    print("Warning: Sum of Net positions in max flow calculation is not zero: [mtu][CNEC name]: " + str(mtu), str(cnec['cnecName']))
                    IDmaxFlows.append(99999)
                    IDmaxFlowNPs.append(None)
                else:
                    IDmaxFlows.append(-1*res.fun)
                    IDmaxFlowNPs.append(NPs)
        else:
            IDmaxFlows.append(None)
            IDmaxFlowNPs.append(None)
    fbdf['IDmaxFlow'] = IDmaxFlows

    return fbdf






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
    
    npdf = pd.read_excel(path + '\\' + 'MarketResults_Week_50_20.xlsx', sheet_name='NetPositions')
    
    fbdf = appendDAmarketCouplingFlows(npdf, fbdf)
    
    # fbdf = appendNtcAac(fbdf, atcdf, bzbs, 0.05)
    
    # fbdf = appendMaximumIntradayFlows(fbdf, atcdf)
    
    
    
    print('Writing output...')
    fbdf.to_csv(path + "\\atce_validation_prototype.csv")
    print('done!')
    
    
