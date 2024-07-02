#!/usr/bin/python
'''
atce_graphical_report
'''
import os
import json
from shutil import which
import numpy as np
import pandas as pd

import pytz
from datetime import datetime
from datetime import timedelta
import requests
from requests.adapters import HTTPAdapter, Retry
import xml.etree.ElementTree as ET


# from entsoe import EntsoePandasClient as Entsoe
# from entsoe import exceptions as ee
# import datetime
import matplotlib.pyplot as plt


def load_json(fileName:str):
    with open(fileName, 'r') as j:
        obj = json.load(j)
    return obj


def query_border(bz_from, bz_to, t_start, t_end, tp_token):
    '''
    e = Entsoe(api_key=tp_token, retry_count=20, retry_delay=30)
    
    s = e.query_intraday_offered_capacity(
        country_code_from=bz_from,
        country_code_to=bz_to,
        start=t_start,
        end=t_end
        )
    '''
    


    assert(t_start.tz is pytz.UTC)
    assert(t_end.tz is pytz.UTC)
    

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    retries = Retry(
        total=25,
        allowed_methods={'POST'},
    )

    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    
    token = tp_token
    indomain = bz_to
    outdomain = bz_from
    
    periodStart = t_start
    
    newTpCutOverDate = datetime(2024,5,17,0,0, tzinfo=pytz.UTC)

    timeStamps = []
    quantities = []
    
    ns = {"xmlns":"{urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3}"}
    time_resolution = {"PT60M": timedelta(seconds=3600), "PT15M": timedelta(seconds=900)}
    
    while periodStart<t_end:
        periodEnd = periodStart + timedelta(days=1)
        Update_DateAndOrTime = periodStart
        
        if periodStart < newTpCutOverDate:

            url = 'https://web-api.tp.entsoe.eu/api?securityToken=' + tp_token \
            + '&documentType=A31&contract_MarketAgreement.Type=A07&in_Domain=' + indomain \
            + '&out_Domain=' + outdomain \
            + '&auction.Type=A01&periodStart=' + periodStart.strftime('%Y%m%d%H%M') \
            + '&periodEnd=' + periodEnd.strftime('%Y%m%d%H%M') #\
            #+ '&Update_DateAndOrTime=' + Update_DateAndOrTime.strftime('%Y%m%d%H%M')

        else:
            url = 'https://web-api.tp.entsoe.eu/api?securityToken=' + tp_token \
            + '&DocumentType=A31&Auction.Type=A08&Out_Domain=' + outdomain \
            + '&In_Domain=' + indomain \
            + '&PeriodStart=' + periodStart.strftime('%Y%m%d%H%M') \
            + '&PeriodEnd=' + periodEnd.strftime('%Y%m%d%H%M') #\
            #+ '&Update_DateAndOrTime=' + Update_DateAndOrTime.strftime('%Y%m%d%H%M')
        
               
        try:
            response = session.get(url)
        except Exception as errMsg:
            print(errMsg)
            ii = 0
            while response is None and ii<5:
                print('HTTP request closed for some reason... Let\'s just forget about that and try one more time...')
                response = session.get(url)
                ii += 1
        
        if response.status_code:
            tree = ET.fromstring(response.content)
                
            ts = tree.findall(ns["xmlns"] + 'TimeSeries')
            
            timeIntervalStart = ts[0].find(ns["xmlns"] + 'Period').find(ns["xmlns"] + 'timeInterval').find(ns["xmlns"] + 'start').text
            timeIntervalResolution = ts[0].find(ns["xmlns"] + 'Period').find(ns["xmlns"] + 'resolution').text

            t_start = datetime.strptime(timeIntervalStart, "%Y-%m-%dT%H:%MZ")
            resolution = time_resolution[timeIntervalResolution]
            
            points = ts[0].find(ns["xmlns"] + 'Period').findall(ns["xmlns"] + 'Point')
            
            for p in points:
                position = int(p.find(ns["xmlns"] + 'position').text)
                quantity = float(p.find(ns["xmlns"] + 'quantity').text)
                
                timeStamps.append(t_start + resolution*(position-1))
                quantities.append(quantity)
            
        periodStart = periodStart + timedelta(days=1)
        response = None
    
    # end while periodStart<t_end
        
    s = pd.DataFrame(quantities, index=timeStamps)

    session.close()
    
    return s



def get_id_offered_atcs(t_start, t_end, tp_token, topology):

    bzb_map = []
    for b in topology['biddingZoneBorders']:
        if not b['type']=='lineset':
            eic_from = next( (z['eic'] for z in topology['biddingZones'] if z['shortName']==b['from']) ,None )
            eic_to = next( (z['eic'] for z in topology['biddingZones'] if z['shortName']==b['to']) ,None )
            a = {'norcapCode': b['name'], 'mappedBorders':[ { "EICfrom": eic_from,"EICto": eic_to } ]}
            bzb_map.append(a)
    
    
    df = pd.DataFrame()
    not_found = []
    for bzb in bzb_map:
        print(bzb["norcapCode"] + ":")
        for mb in bzb["mappedBorders"]:
            try:
                se = query_border(mb["EICfrom"], mb["EICto"], t_start, t_end, tp_token)
                df[bzb["norcapCode"]] = se                
            # except ee.NoMatchingDataError:
                # print("NoMatchingDataError: Border capacity query for " + bzb['norcapCode'] + " did not return any result.")
                # not_found.append(bzb['norcapCode'])
            except IndexError:
                print("KeyError: Border capacity query for " + bzb['norcapCode'] + " did not return any result.")
                not_found.append(bzb['norcapCode'])
            except Exception as errMsg:
                print(errMsg)
        

    return df




def readATCEextracts(folder):
    files = os.listdir(os.getcwd() + "\\" + folder)
    # files = [f for f in files if "extract.csv" in f]   
    files = [f for f in files if ".csv" in f]   
    
    seperator = ','
    
    df = pd.read_csv(folder + "\\" + files[0], header=[0, 1], sep=seperator)
    if len(files)>1:
        for file in files[1::]:
            df = pd.concat([df, pd.read_csv(folder + "\\" + file, header=[0, 1], sep=seperator)])
    
    df["dateTime"] = pd.to_datetime(df["MTU"]["MTU"], format="%Y-%m-%dT%H:%MZ", utc=True)
    
    # df = pd.read_csv(file, header=[0, 1], sep=";")
    # print(df.columns)
    # df["dateTime"] = pd.to_datetime(df["MTU"]["MTU"], format="%Y-%m-%dT%H:%MZ", utc=True)
    
    return df
    



def plotNTCs(df, topology):
    # plotNTCs(df, border_directions)
    # plots NTC values for each border direction as a stacked surface plot.
    
    border_directions = [b['name'] for b in topology['biddingZoneBorders']]
    
    df = df.sort_values(by='dateTime',ascending=True)
    
    # backupDates = df["dateTime"].loc[df["Backup"]["Backup"]]    
    df_good = df.copy()
    df_bad = df.copy()
    # for b in border_directions:
        # df_good.loc[df_good["Backup"]["Backup"] == True, (b, "ATC")] = 0
        # df_bad.loc[df_bad["Backup"]["Backup"] == False, (b, "ATC")] = 0

    borderLockIn = {b:[] for b in border_directions}
    for bzb in border_directions:
        bzbs = bzb.split("-")
        bzb_reverse = bzbs[1] + "-" + bzbs[0]
        
        plt.stackplot(df_good["dateTime"], df_good[bzb]["NTC_final"], -1*(df_good[bzb]["ATC"]+df_good[bzb_reverse]["ATC"]), colors = ["none", [0.1,0.5,0.3,0.5]],edgecolor="none" )
        # if len(backupDates)>0:
            # plt.stackplot(df_bad["dateTime"], df_bad[bzb]["NTC_final"], -1*(df_bad[bzb]["ATC"]+df_bad[bzb_reverse]["ATC"]), colors = ["none", [0.5,0.0,0.1,0.3]],edgecolor="none" )                    
            # legend = ["_nolegend_", "NorCap ATCE","_nolegend_", "ATCE backup", "Already allocated flow"]
        # else:
            # legend = ["_nolegend_", "NorCap ATCE", "Already allocated flow"]
        legend = ["_nolegend_", "NorCap ATCE", "Already allocated flow"]
        plt.grid()
        plt.scatter(df["dateTime"], df[bzb]["AAC"], c=[[0.1,0.5,0.3,1.0]], marker="+")
        plt.legend(legend)
        plt.xticks(rotation=45, ha='right')
        plt.title("NTC " + bzb)
        plt.ylabel("NTC [MW]")
        plt.tight_layout()
        plt.savefig(folder + "\\" + bzb + ".pdf", format="pdf")
        plt.close()
        

def bzDurationCurves(df, reference_df, topology, topology_map):
    #  bidding_zones, border_directions

    real_nordic_bidding_zones = [bz['shortName'] for bz in topology['biddingZones'] if (not bz['isVirtual'] and bz['ccr']=='nordics')]

    upwardLockIn = {b:[] for b in real_nordic_bidding_zones}
    downwardLockIn = {b:[] for b in real_nordic_bidding_zones}
    biDirectionalLockIn = {b:[] for b in real_nordic_bidding_zones}
    
    for bz in real_nordic_bidding_zones:
        out_borders = [b['name'] for b in topology['biddingZoneBorders'] if (b['from']==bz and not b['type']=='lineset')]
        in_borders = [b['name'] for b in topology['biddingZoneBorders'] if (b['to']==bz and not b['type']=='lineset')]

        dur_curve = []
        dur_curve_imp = []
        dur_curve_exp = []
        missing_reference_ob = []
        missing_reference_ib = []
        
        print(bz, "out: ",out_borders)
        print(bz, "in: ",in_borders)
        
        for idx, r in df.iterrows():
            export_capacity = sum([r[ob]["ATC"]for ob in out_borders])
            import_capacity = sum([r[ib]["ATC"] for ib in in_borders])
            # if not bool(r["Backup"]["Backup"]):
            dur_curve.append(export_capacity + import_capacity)
            dur_curve_exp.append(export_capacity)
            dur_curve_imp.append(import_capacity)
            if export_capacity <1:
                upwardLockIn[bz].append(r["MTU"][0])
            if import_capacity<1:
                downwardLockIn[bz].append(r["MTU"][0])
            if export_capacity < 1 and import_capacity < 1:
                biDirectionalLockIn[bz].append(r["MTU"][0])

        reference_dur_curve = []
        reference_dur_curve_exp = []
        reference_dur_curve_imp = []
        for idx, r in reference_df.iterrows():
            ref_export_capacity = 0
            for ob in out_borders:
                mapped_ob = next(( r['key'] for r in topology_map['map'] if r['value'] == ob ),None)
                if not mapped_ob is None:
                    if mapped_ob in r:
                        ref_export_capacity += np.float64(r[mapped_ob])
                    else:
                        missing_reference_ob.append(mapped_ob)
            ref_import_capacity = 0
            for ib in in_borders:
                mapped_ib = next(( r['key'] for r in topology_map['map'] if r['value'] == ib ),None)
                if not mapped_ib is None:
                    if mapped_ib in r:
                        ref_import_capacity += np.float64(r[mapped_ib])
                    else:
                        missing_reference_ib.append(mapped_ib)    

            reference_dur_curve.append(ref_export_capacity + ref_import_capacity)
            reference_dur_curve_exp.append(ref_export_capacity)
            reference_dur_curve_imp.append(ref_import_capacity)
        
        
        dur_curve = sorted(dur_curve)
        missing_reference_ib = set(missing_reference_ib)
        missing_reference_ob = set(missing_reference_ob)
        
        reference_dur_curve = [x for x in reference_dur_curve if x==x]
        reference_dur_curve = sorted(reference_dur_curve)
        plt.plot([100*x/len(dur_curve) for x in range(len(dur_curve))], dur_curve, label="NorCap ATCE")
        plt.plot([100*x/len(reference_dur_curve) for x in range(len(reference_dur_curve))], reference_dur_curve, label="Current method")
        title_text = "ATC trading space BZ: " + bz
        if len(missing_reference_ob)>0 or len(missing_reference_ib)>0:
            title_text += " Note! Data for following borders were missing from reference data: " + ', '.join(missing_reference_ob)+ ', ' + ', '.join(missing_reference_ib)
        plt.title(title_text, wrap=True)
        plt.xlabel("Percent of MTUs")
        plt.ylabel("ATC export + ATC import [MW]")
        plt.legend()
        plt.xlim((0,100))
        if not len(missing_reference_ib)>0 or not len(missing_reference_ob)>0:
            plt.tight_layout()      
        plt.savefig(folder + "\\" + bz + "_trading_space.pdf", format="pdf")
        plt.close()
        
        plt.plot([100*x/len(dur_curve_imp) for x in range(len(dur_curve_imp))], sorted(dur_curve_imp), label="NorCap ATCE")
        plt.plot([100*x/len(reference_dur_curve_imp) for x in range(len(reference_dur_curve_imp))], sorted(reference_dur_curve_imp), label="Current method")
        title_text = "Importing ATC trading space BZ: " + bz
        
        if len(missing_reference_ib)>0:
            title_text += " Note! Data for following borders were missing from reference data: " + ', '.join(missing_reference_ib)
        print(title_text)
        plt.title(title_text, wrap=True)
        plt.xlabel("Percent of MTUs")
        plt.ylabel("ATC import [MW]")
        plt.legend()
        plt.xlim((0,100))
        if not len(missing_reference_ib)>0 or not len(missing_reference_ob)>0:
            plt.tight_layout()        
        plt.savefig(folder + "\\" + bz + "_import_trading_space.pdf", format="pdf")
        plt.close()

        plt.plot([100*x/len(dur_curve_exp) for x in range(len(dur_curve_exp))], sorted(dur_curve_exp), label="NorCap ATCE")
        plt.plot([100*x/len(reference_dur_curve_exp) for x in range(len(reference_dur_curve_exp))], sorted(reference_dur_curve_exp), label="Current method")
        title_text = "Exporting ATC trading space BZ: " + bz

        if len(missing_reference_ob)>0:
            title_text += " Note! Data for following borders were missing from reference data: " + ', '.join(missing_reference_ob)
        plt.title(title_text, wrap=True)
        plt.xlabel("Percent of MTUs")
        plt.ylabel("ATC export [MW]")
        plt.legend()
        plt.xlim((0,100))
        if not len(missing_reference_ib)>0 or not len(missing_reference_ob)>0:
            plt.tight_layout()        
        plt.savefig(folder + "\\" + bz + "_export_trading_space.pdf", format="pdf")
        plt.close()
    
    
    return upwardLockIn, downwardLockIn, biDirectionalLockIn


def bzbDurationCurves(df, reference_df,  topology, topology_map):
    # border_directions
    
    
    borderLockIn = {b['name']:[] for b in topology['biddingZoneBorders']}
    for bzb in topology['biddingZoneBorders']:
        
        bzb_reverse = next((b['name'] for b in topology['biddingZoneBorders'] if (b['from']==bzb['to'] and b['to']==bzb['from'] and b['type']==bzb['type'])),None)
        if bzb_reverse is None:
            print(bzb['name'])
        for idx, r in df.iterrows():
            if bzb['name'] in r and bzb_reverse in r:
                if r[bzb['name']]["ATC"]<1 and r[bzb_reverse]["ATC"]<1:
                    if r[bzb['name']]["NTC_final"]>1 or r[bzb_reverse]["NTC_final"]>1:
                        borderLockIn[bzb['name']].append(r["MTU"][0])

        
        plt.title(bzb['name'])
        dur_curve = df[bzb['name']]["ATC"].sort_values()
        
        plt.plot([100*x/len(dur_curve) for x in range(len(dur_curve))], dur_curve, label="Extracted ATC")
        
        mapped_bzb = next((b['key'] for b in topology_map['map'] if b['value']==bzb['name']),None)
        if not mapped_bzb is None:
            try:
                ref = reference_df[mapped_bzb]
                plt.plot([100*x/len(ref) for x in range(len(ref))], ref.sort_values(), label="Current method")
            except KeyError:
                print(mapped_bzb + " was not added to bzb capacity duration curve, due to missing data.")
        
        plt.legend()
        plt.xlabel("Percent of MTUs")
        plt.ylabel("ATC [MW]")
        plt.xlim((0,100))
        plt.tight_layout()
        plt.savefig(folder + "\\" + bzb['name'] + "_atc_duration_curve.pdf", format="pdf")
        plt.close()

    return borderLockIn


def makePresentation(folder, df, upwardLockIn, downwardLockIn, biDirectionalLockIn, borderLockIn, topology):
    backupDates = []#df["dateTime"].loc[df["Backup"]["Backup"]]
    
    bidding_zones = [b['shortName'] for b in topology['biddingZones'] if (not b['isVirtual'] and b['ccr']=='nordics' )]
    border_directions = []
    
    bzbs = topology['biddingZoneBorders']
    
    for bzb in bzbs:
        border_directions.append(bzb['name'])
        reverse_border = next(( b for b in topology['biddingZoneBorders'] if (b['from']==bzb['to'] and b['to']==bzb['from'] and b['type']==bzb['type'])), None)
        border_directions.append(reverse_border['name'])
        bzbs.pop(bzbs.index(reverse_border))
    
    
    texfilename = folder.split("\\")[-1] + "_ATCE_results.tex"
    with open(folder + "\\" + texfilename, "w+") as f:
        f.write("\\documentclass{beamer}\n")
        f.write("\\mode<presentation>\n{\n\\usetheme{default}\n\\setbeamercovered{transparent}\n}")
        f.write("\\usepackage[english]{babel}\n\\usepackage[utf8]{inputenc}\n\\usepackage{times}\n\\usepackage[T1]{fontenc}\n\\usepackage{graphicx}\n\\title[]{" + folder.replace("_", " ").split("\\")[-1] + " ATC Extraction Results}\n\\author[NRCC]{Nordic RCC}\n")
        f.write("\n\n\\begin{document}\n\n")
        f.write("\\begin{frame}\n")
        f.write("\\titlepage\n" )
        f.write("\\end{frame}\n\n") #
        f.write("\n\n\\IfFileExists{./comments.tex}{\\input{comments}}{}\n\n")
        f.write("\\begin{frame}{Bidding zone lock-in statistics}\n")
        f.write("{\\tiny An area operates at maximum export when the sum of ATC on all exporting directions of that area is less than 1MW.\\\\ An area operates at maximum import when the sum of ATC on all importing directions of that area is less than 1MW.\\\\ An area operates in lock-in if during the same MTU it is operating at both maximum export and maximum import.")
        
        if len(backupDates)>0:
            f.write("\\\\"+str(len(backupDates))+" MTUs where ATCE failed have been excluded from the statistics.")
        
        f.write("}\n")
        f.write("\\centering\n")
        f.write("\\begin{tabular}{l|p{0.2\\textwidth}|p{0.2\\textwidth}|p{0.2\\textwidth}}\n")
        f.write("Bidding zone & \\#MTUs at max Export & \\#MTUs at max Import & \\#MTUs at lock-in \\\\ \\hline\n")
        
        for bz in bidding_zones:
            f.write(bz + " & " + str(len(upwardLockIn[bz])) + " & " + str(len(downwardLockIn[bz])) + " & " + str(len(biDirectionalLockIn[bz])) + "\\\\ \\hline\n")
        
        f.write("\\end{tabular}\n")
        f.write("\\end{frame}\n")
        f.write("\\begin{frame}{Border lock-in statistics}\n")
        f.write("{\\tiny A bidding zone border is operating in a lock-in situation, if at a given MTU, the ATC of the bidding zone border is smaller than 1MW in both forward and reverse trading direction.")
        
        if len(backupDates)>0:
            f.write("\\\\ "+str(len(backupDates))+" MTUs where ATCE failed have been excluded from the statistics.")
        
        f.write("}\n")
        f.write("\\centering\n")
        f.write("\\begin{tabular}{l|r}\n")
        f.write("Border & \\#MTUs at lock-in \\\\ \\hline\n")
        
        rowCount = 0
        maxRows = 12
        for bzb in border_directions:
            if len(borderLockIn[bzb]) > 0:
                f.write(bzb.replace("_", "\_") + " & " + str(len(borderLockIn[bzb])) + "\\\\ \\hline\n")
                rowCount += 1
                
                if rowCount%maxRows < 1:
                    f.write("\\end{tabular}\n")
                    f.write("\\end{frame}\n")
                    f.write("\\begin{frame}{Border lock-in statistics}\n")
                    f.write("{\\tiny A bidding zone border is operating in a lock-in situation, if at a given MTU, the ATC of the bidding zone border is smaller than 1MW in both forward and reverse trading direction.}\n")
                    f.write("\\centering\n")
                    f.write("\\begin{tabular}{l|r}\n")
                    f.write("Border & \\#MTUs at lock-in \\\\ \\hline\n")                    
        
        f.write("\\end{tabular}\n")
        f.write("\\end{frame}\n\n")
        
        f.write("\\begin{frame}{Border NTC plots - Reader's guide}\n")
        f.write("\\includegraphics[width=0.5\\textwidth]{" + border_directions[15].replace("_", "\\_") + ".pdf}\n" )
        f.write("\\newline {\\tiny The colored area represents the possible exchange on this border and direction. The cross marks the simulated day ahead market coupling flows. Any colored area above the cross, means that intraday market will be able to increase exchange over the day ahead market coupling flows. Any colored area below the cross means that the intraday market will be able to trade against the day ahead market.}\n")
        f.write("\\end{frame}\n\n")
        

        for b in border_directions:
            mapped_b = next(( k['value'] for k in topology_map['map'] if k['key']==b),None)
            f.write("\\begin{frame}{" + b.replace("_", "\\_") + "}\n")
            # if b == "NO2-NO2_ND" or b == "NO2_ND-NO2":
                # f.write("\\includegraphics[width=0.8\\textwidth]{" + b.replace("_", "\\_") + ".pdf}\n" )
                # f.write("\\newline {\\tiny Note: NTC for NorNed includes 3.1\\% capacity reserved for losses. These will be subtracted in a future revision.}\n")
            # else:
            f.write("\\includegraphics[width=\\textwidth]{" + b.replace("_", "\\_") + ".pdf}\n" )
            
            
            f.write("\\end{frame}\n\n")
        
        f.write("\\begin{frame}\n")
        f.write("\\begin{Large}Bidding zone trading space\\end{Large}\n" )
        f.write("\\begin{tiny}\n")
        f.write("\\newline The total trading space of a bidding zone for a given MTU is the sum of export capacity and import capacity on all borders of that bidding zone for that MTU. The directional trading space is the sum of ATC on all borders of a bidding zone in either exporting or importing direction.\n")
        f.write("\\newline \\underline{Disclaimer:} Trading space computed by the reference method (i.e. the current method used in production) are calculated from intra-day offered ATCs collected from ENTSO-e transparency platform. It must be noted that the capacities collected at transparency platform are harmonized capacities including limimtations submitted by non-Nordic TSOs and ramping constraints for some HVDCs.")
        f.write("\\end{tiny}\n")
        f.write("\\end{frame}\n\n")            

        for bz in bidding_zones:
            f.write("\\begin{frame}{" + bz.replace("_", "\\_") + " - Total trading space}\n")
            f.write("\\centering\n")
            f.write("\\includegraphics[width=0.75\\textwidth]{" + bz + "_trading_space.pdf}\n" )
            
            if len(backupDates) > 0:
                f.write("\\newline {\\small Note: MTUs where ATC extraction resulted in use of backup have been excluded from the trading space curves.}\n")
            
            f.write("\\end{frame}\n\n")
            f.write("\\begin{frame}{" + bz.replace("_", "\\_") + " - Directional trading space}\n")
            f.write("\\begin{columns}\n")
            f.write("\\begin{column}{0.5\\textwidth}\n")
            f.write("\\centering\n")
            f.write("\\includegraphics[width=\\textwidth]{" + bz + "_export_trading_space.pdf}\n" )
            f.write("\\end{column}\n")
            f.write("\\begin{column}{0.5\\textwidth}\n")
            f.write("\\centering\n")
            f.write("\\includegraphics[width=\\textwidth]{" + bz + "_import_trading_space.pdf}\n" )
            f.write("\\end{column}\n")
            f.write("\\end{columns}\n")
            
            if len(backupDates) > 0:
                f.write("{\\small Note: MTUs where ATC extraction resulted in use of backup have been excluded from the trading space curves.}\n")
            
            f.write("\\end{frame}\n\n")            
            
        f.write("\\begin{frame}\n")
        f.write("\\centering\n\\begin{Large}Capacity duration curves\\end{Large}\n" )
        f.write("\\end{frame}\n\n")            
            
        for b in border_directions:
            f.write("\\begin{frame}{" + b.replace("_", "\\_") + " ATC duration curves}\n")
            f.write("\\centering\n")
            f.write("\\includegraphics[width=0.85\\textwidth]{" + b.replace("_", "\\_") + "_atc_duration_curve" + ".pdf}\n" )
            if len(backupDates) > 0:
                f.write("\\newline {\\small Note: MTUs where ATC extraction resulted in use of backup have been excluded from the capacity duration curves.}\n")
            f.write("\\end{frame}\n\n")            
        
        f.write("\n\n\\end{document}")
    
    return texfilename


def compileTexFile(folder, texfilename):
    if which('pdflatex') is None:
        print('... hmmm it appears that pdflatex is no installed or not found in PATH... Compile skiped.')
    else:
        topDir = os.getcwd()
        os.chdir(folder)
        os.system("pdflatex " + texfilename)
        os.chdir(topDir)
    return    


def getTPtoken(fullDir):
    with open(fullDir) as f:
        for line in f:
            if not "tpSecurityToken" in line:
                None
            else:
                token = line.split("=")[1]
                break
                
    return token
                




if __name__=="__main__":
    
    tp_token = getTPtoken("entsoeTransparencyToken.txt")
 
    folder = '..\\data\\2024w21'
    
    path_to_topology = '..\\topology\\data\\'
    
    ID_topology_file = 'topology_intradayNTC.JSON'
    TP_topology_file = 'topology_entsoeTP.JSON'
    topology_map_file = 'map_entsoeTP-intradayNTC.JSON'
    
    
    ID_topology = load_json( path_to_topology + ID_topology_file)
    TP_topology = load_json( path_to_topology + TP_topology_file)
    
    topology_map = load_json( path_to_topology + topology_map_file)

    print(folder)
    df = readATCEextracts(folder)
    df = df.fillna(0)
    
    
    plotNTCs(df, ID_topology)
    
    reference_start = df["dateTime"].min()
    reference_end = df["dateTime"].max()

    reference_df = get_id_offered_atcs(reference_start, reference_end, tp_token, TP_topology)
    reference_df = reference_df.fillna(0)
    
    ''' Note: TP contains one listing of capacities SE3-SE4, which corresponds to eics of SE3_AC-SE4_AC in topology. Translation is done to refer capacities to the lineset SE3-SE4'''
    reference_df['SE3-SE4'] = reference_df['SE3_AC-SE4_AC']
    reference_df['SE4-SE3'] = reference_df['SE4_AC-SE3_AC']
    
    
    reference_df.to_csv("reference_df.csv")
    # reference_df = pd.read_csv('reference_df.csv')
    upwardLockIn, downwardLockIn, biDirectionalLockIn = bzDurationCurves(df, reference_df, ID_topology, topology_map)
    
    borderLockIn = bzbDurationCurves(df, reference_df, ID_topology, topology_map)
    
    texfilename = makePresentation(folder, df, upwardLockIn, downwardLockIn, biDirectionalLockIn, borderLockIn, ID_topology)
    
    compileTexFile(folder, texfilename)
    del reference_df
    del df
