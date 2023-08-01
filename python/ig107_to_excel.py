#!/usr/bin/python
'''
Script for parsing ig-107 xml file to csv format.

'''
import os
import zipfile
from datetime import datetime
from datetime import timedelta
import xml.etree.ElementTree as ET
import topology

time_resolution = {"PT60M": timedelta(seconds=3600), "PT15M": timedelta(seconds=900)}
bt2code = {"A25":"CNTC_IVA", "A26":"ATC", "A27":"NTC_final", "A29":"AAC", "B38":"NTC_initial"}
code2bt = {"CNTC_IVA":"A25", "ATC":"A26", "NTC_final":"A27", "AAC":"A29", "NTC_initial":"B38"}
#code2eic = {"SE1":"10Y1001A1001A44P", "SE2":"10Y1001A1001A45N", "SE3":"10Y1001A1001A46L", "SE4":"10Y1001A1001A47J", "NO5":"10Y1001A1001A48H", "NO1A":"10Y1001A1001A64J", "DK1A":"10YDK-1-------AA", "DK1":"10YDK-1--------W", "DK2":"10YDK-2--------M", "FI":"10YFI-1--------U", "NO1":"10YNO-1--------2", "NO2":"10YNO-2--------T", "NO3":"10YNO-3--------J", "NO4":"10YNO-4--------9", "FI_EL":"44Y-00000000161I", "DK1_CO":"45Y0000000000046", "DK1_DE":"45Y0000000000054", "DK2_KO":"45Y0000000000070", "SE4_SP":"46Y000000000003U", "SE4_NB":"46Y000000000004S", "NO2_ND":"50Y73EMZ34CQL9AJ", "NO2_DE":"50YNBFFTWZRAHA3P", "10Y1001A1001A44P":"SE1", "10Y1001A1001A45N":"SE2", "10Y1001A1001A46L":"SE3", "10Y1001A1001A47J":"SE4", "10Y1001A1001A48H":"NO5", "10Y1001A1001A64J":"NO1A", "10YDK-1-------AA":"DK1A", "10YDK-1--------W":"DK1", "10YDK-2--------M":"DK2", "10YFI-1--------U":"FI", "10YNO-1--------2":"NO1", "10YNO-2--------T":"NO2", "10YNO-3--------J":"NO3", "10YNO-4--------9":"NO4", "44Y-00000000161I":"FI_EL", "45Y0000000000046":"DK1_CO", "45Y0000000000054":"DK1_DE", "45Y0000000000070":"DK2_KO", "46Y000000000003U":"SE4_SP", "46Y000000000004S":"SE4_NB", "50Y73EMZ34CQL9AJ":"NO2_ND", "50YNBFFTWZRAHA3P":"NO2_DE", '50YCUY85S1HH29EK':'NO2-NO2_SK'}

shortname=[]
eic=[]
for t in topology.topology20230511["biddingZones"]:
   eic.append(t['eic'])
   shortname.append(t["norCapShortName"])
   

code2eic={eic[t]:shortname[t] for t in range(len(eic))}
'''
code2eic = {"10YDK-1--------W": "DK1",
            "10YDK-2--------M": "DK2",
            "10YFI-1--------U": "FI",
            "10YNO-1--------2": "NO1",
            "10YNO-2--------T": "NO2",
            "10YNO-3--------J": "NO3",
            "10YNO-4--------9": "NO4",
            "10Y1001A1001A48H": "NO5",
            "10Y1001A1001A44P": "SE1",
            "10Y1001A1001A45N": "SE2",
            "10Y1001A1001A46L": "SE3",
            "10Y1001A1001A47J": "SE4",
            "45Y0000000000046": "DK1_CO",
            "45Y0000000000054": "DK1_DE",
            "45Y000000000002A": "DK1_KS",
            "45Y0000000000038": "DK1_SB",
            "45Y000000000001C": "DK1_SK",
            "45Y0000000000070": "DK2_KO",
            "45Y0000000000062": "DK2_SB",
            "44Y-00000000161I": "FI_EL",
            "44Y-00000000160K": "FI_FS",
            "50Y73EMZ34CQL9AJ": "NO2_ND",
            "50YNBFFTWZRAHA3P": "NO2_NK",
            "50Y0JVU59B4JWQCU": "NO2_NSL",
            "50YCUY85S1HH29EK": "NO2_SK",
            "46Y000000000001Y": "SE3_FS",
            "46Y000000000002W": "SE3_KS",
            "46Y000000000007M": "SE3_SWL",
            "46Y000000000005Q": "SE4_BC",
            "46Y000000000004S": "SE4_NB",
            "46Y000000000003U": "SE4_SP",
            "46Y000000000008K": "SE4_SWL",
            "10Y1001A1001A64J":"NO1A",
            "10YDK-1-------AA":"DK1A",
            "10YSE3A-TEMP--AA":"SE3A",
            "46Y000000000015N":"SE3_ACDC",
            "46Y000000000016L":"SE4_ACDC",
            "DK1": "10YDK-1--------W",
            "DK2": "10YDK-2--------M",
            "FI": "10YFI-1--------U",
            "NO1": "10YNO-1--------2",
            "NO2": "10YNO-2--------T",
            "NO3": "10YNO-3--------J",
            "NO4": "10YNO-4--------9",
            "NO5": "10Y1001A1001A48H",
            "SE1": "10Y1001A1001A44P",
            "SE2": "10Y1001A1001A45N",
            "SE3": "10Y1001A1001A46L",
            "SE4": "10Y1001A1001A47J",
            "DK1_CO": "45Y0000000000046",
            "DK1_DE": "45Y0000000000054",
            "DK1_KS": "45Y000000000002A",
            "DK1_SB": "45Y0000000000038",
            "DK1_SK": "45Y000000000001C",
            "DK2_KO": "45Y0000000000070",
            "DK2_SB": "45Y0000000000062",
            "FI_EL": "44Y-00000000161I",
            "FI_FS": "44Y-00000000160K",
            "NO2_ND": "50Y73EMZ34CQL9AJ",
            "NO2_NK": "50YNBFFTWZRAHA3P",
            "NO2_NSL": "50Y0JVU59B4JWQCU",
            "NO2_SK": "50YCUY85S1HH29EK",
            "SE3_FS": "46Y000000000001Y",
            "SE3_KS": "46Y000000000002W",
            "SE3_SWL": "46Y000000000007M",
            "SE4_BC": "46Y000000000005Q",
            "SE4_NB": "46Y000000000004S",
            "SE4_SP": "46Y000000000003U",
            "SE4_SWL": "46Y000000000008K",
            "NO1A":"10Y1001A1001A64J",
            "DK1A":"10YDK-1-------AA",
            "SE3A":"10YSE3A-TEMP--AA",
            "SE3_ACDC":"46Y000000000015N",
            "SE4_ACDC":"46Y000000000016L"}
'''


def parseTimeSeriesFromCapacityDocument(ig107File):
    # parse all time series defined in implementation guide for ig-107 to a list of dicts.
    ns = {"xmlns":"{urn:iec62325.351:tc57wg16:451-3:capacitydocument:8:0}", "cimp":"{http://www.iec.ch/cimprofile}", "xsi": "{http://www.w3.org/2001/XMLSchema-instance}"}
    tree = ET.parse(ig107File)
    root = tree.getroot()
    time_series = root.findall(ns["xmlns"] + "TimeSeries")
    ts_list = []
    for t in time_series:
        p = t.find(ns["xmlns"] + "Period")
        ts = {"borderDirection": code2eic[t.find(ns["xmlns"] + "out_Domain.mRID").text] + "-" + code2eic[t.find(ns["xmlns"] + "in_Domain.mRID").text],
            "BusinessType":t.find(ns["xmlns"] + "businessType").text,
            "InDomain":t.find(ns["xmlns"] + "in_Domain.mRID").text,
            "OutDomain":t.find(ns["xmlns"] + "out_Domain.mRID").text,
            "period.timeinterval.start": datetime.strptime(p.find(ns["xmlns"]+"timeInterval").find(ns["xmlns"]+"start").text, "%Y-%m-%dT%H:%MZ"),
            "period.timeinterval.end": datetime.strptime(p.find(ns["xmlns"]+"timeInterval").find(ns["xmlns"]+"end").text, "%Y-%m-%dT%H:%MZ"),
            "period.resolution":p.find(ns["xmlns"] + "resolution").text}
            
        curvePoints = []
        CNTC_backup = []
        for cp in p.findall(ns["xmlns"] + "Point"):
            point_datetime = ts["period.timeinterval.start"] + (int(cp.find(ns["xmlns"]+"position").text) - 1)*time_resolution[ts["period.resolution"]]
            curvePoints.append((point_datetime,float(cp.find(ns["xmlns"]+"quantity").text)))
            try:
                reason = cp.find(ns["xmlns"]+"Reason").find(ns["xmlns"]+"text").text
                CNTC_backup.append((point_datetime, "BACKUP" in reason or "FALLBACK" in reason))
            except Exception as errmsg:
                CNTC_backup.append((point_datetime,False))
        
        ts["curve"] = curvePoints
        ts["backup"] = CNTC_backup
        ts_list.append(ts)
        
    return ts_list
    
    

def extract_ig107_files(files, bzb_order=None):
    # For each ig107 file parse content to csv format.
    # If a bzb_order is provided, column order in output csv will follow the bzb_order. Although, if a border does not exist in time series data, it will be skipped in csv parsing.
    outFileNames = []
    
    for  ig107File in files:
        print("Parsing file: " + ig107File + "...")
        ts = parseTimeSeriesFromCapacityDocument(ig107File)
        
        ts = [ii for ii in ts if ii["period.resolution"]=="PT60M"]
        timeStamps = list(list(zip(*ts[0]["curve"]))[0])
        
        if bzb_order is None:
            bzb_list = set([ii["borderDirection"] for ii in ts])
            outFileName = ig107File.replace(".xml", "_full_extract.csv")
        else:        
            bzb_list = bzb_order
            outFileName = ig107File.replace(".xml", "_public_extract.csv")
            
        field_order = ["NTC_initial", "CNTC_IVA","NTC_final","AAC","ATC"]

        outFileNames.append(outFileName)

        with open(outFileName,"w+") as fid:
            fid.write("MTU,Backup,")
            for bzb in bzb_list:
                for field in field_order:
                    c = next((item["borderDirection"] for item in ts if item["borderDirection"]==bzb and item["BusinessType"]==code2bt[field]), None)
                    if not c == None:
                        fid.write(c + ",")
            fid.write("\nMTU,Backup,")
            for bzb in bzb_list:
                for field in field_order:
                    c = next((bt2code[item["BusinessType"]] for item in ts if item["borderDirection"]==bzb and item["BusinessType"]==code2bt[field]), None)
                    if not c == None:
                        fid.write(c + ",")
            fid.write("\n")
            for t in timeStamps:
                backup = False
                fid.write(t.strftime(format="%Y-%m-%dT%H:%MZ")+",")

                for bzb in bzb_list:
                    for field in field_order:
                        infos = next((item["backup"] for item in ts if item["borderDirection"]==bzb and item["BusinessType"]==code2bt[field]), None)
                        if not infos == None:
                            backup = backup or next((point[1] for point in infos if point[0]==t), None)
                    
                fid.write(str(backup) + ",")
                
                for bzb in bzb_list:
                    for field in field_order:
                        c = next((item["curve"] for item in ts if item["borderDirection"]==bzb and item["BusinessType"]==code2bt[field]), None)
                        if not c == None:
                            q = next((point[1] for point in c if point[0]==t), None)
                            #fid.write(q + ',')
                            fid.write('{:.1f},'.format(q))
                fid.write("\n")
    return outFileNames


def zipFiles(zipFileName, path, fileNames, deleteUnzipped=True):
    print("Creating zip archive: " + path + "\\" + zipFileName)
    current_dir = os.getcwd()
    os.chdir(path)
    with zipfile.ZipFile(zipFileName, 'w') as zf:
        for f in fileNames:
            f_name = f.split('\\')[-1]
            print('adding ' + f_name + ' to ' + zipFileName)
            zf.write(f_name)
            if deleteUnzipped and len(f_name) > 0:
                os.remove(f_name)
    os.chdir(current_dir)
    return


if __name__=="__main__":
    
    border_order = ["DK1_CO-DK1", "DK1-DK1_CO", "DK1_DE-DK1", "DK1-DK1_DE", "DK1-DK1A", "DK1A-DK1", "DK1-DK2", "DK2-DK1", "DK1A-NO2", "NO2-DK1A", "DK1A-SE3", "SE3-DK1A", "DK2_KO-DK2", "DK2-DK2_KO", "DK2-SE4", "SE4-DK2", "FI_EL-FI", "FI-FI_EL", "FI-SE1", "SE1-FI", "NO1A-NO1", "NO1-NO1A", "NO1A-NO2", "NO2-NO1A", "NO1A-NO5", "NO5-NO1A", "NO1-NO3", "NO3-NO1", "NO1-SE3", "SE3-NO1", "NO2-NO2_ND", "NO2_ND-NO2", "NO2-NO2_NK", "NO2_NK-NO2", "NO2-NO5", "NO5-NO2", "NO3-NO4", "NO4-NO3", "NO3-NO5", "NO5-NO3", "NO3-SE2", "SE2-NO3", "NO4-SE1", "SE1-NO4", "NO4-SE2", "SE2-NO4", "SE3-FI", "FI-SE3", "SE1-SE2", "SE2-SE1", "SE2-SE3", "SE3-SE2", "SE3-SE4", "SE4-SE3", "SE4-SE4_NB", "SE4_NB-SE4", "SE4-SE4_SP", "SE4_SP-SE4"]
    
    try:
        fDir = "..\\data"
        files = os.listdir(os.getcwd() + "\\"+ fDir)
        files = [fDir + "\\" + f for f in files if f[-3:] == "xml"]
                
        public_files = extract_ig107_files(files, bzb_order = border_order)
        zipFiles(fDir.split('\\')[-1]+'_public.zip', fDir, public_files)
                
        private_files = extract_ig107_files(files)
        zipFiles(fDir.split('\\')[-1]+'_for_TSOs.zip', fDir, private_files, deleteUnzipped=False)
        
    except Exception as errMsg:
        print(errMsg)
    
    
