#!/usr/bin/python
'''
Script for parsing ig-107 xml file to csv format.
'''
import os
from datetime import datetime
from datetime import timedelta
import xml.etree.ElementTree as ET

time_resolution = {"PT60M": timedelta(seconds=3600), "PT15M": timedelta(seconds=900)}
bt2code = {"A25":"CNTC_IVA", "A26":"ATC", "A27":"NTC_final", "A29":"AAC", "B38":"NTC_initial"}
code2bt = {"CNTC_IVA":"A25", "ATC":"A26", "NTC_final":"A27", "AAC":"A29", "NTC_initial":"B38"}
code2eic = {"SE1":"10Y1001A1001A44P", "SE2":"10Y1001A1001A45N", "SE3":"10Y1001A1001A46L", "SE4":"10Y1001A1001A47J", "NO5":"10Y1001A1001A48H", "NO1A":"10Y1001A1001A64J", "DK1A":"10YDK-1-------AA", "DK1":"10YDK-1--------W", "DK2":"10YDK-2--------M", "FI":"10YFI-1--------U", "NO1":"10YNO-1--------2", "NO2":"10YNO-2--------T", "NO3":"10YNO-3--------J", "NO4":"10YNO-4--------9", "FI_EL":"44Y-00000000161I", "DK1_CO":"45Y0000000000046", "DK1_DE":"45Y0000000000054", "DK2_KO":"45Y0000000000070", "SE4_SP":"46Y000000000003U", "SE4_NB":"46Y000000000004S", "NO2_ND":"50Y73EMZ34CQL9AJ", "NO2_DE":"50YNBFFTWZRAHA3P", "10Y1001A1001A44P":"SE1", "10Y1001A1001A45N":"SE2", "10Y1001A1001A46L":"SE3", "10Y1001A1001A47J":"SE4", "10Y1001A1001A48H":"NO5", "10Y1001A1001A64J":"NO1A", "10YDK-1-------AA":"DK1A", "10YDK-1--------W":"DK1", "10YDK-2--------M":"DK2", "10YFI-1--------U":"FI", "10YNO-1--------2":"NO1", "10YNO-2--------T":"NO2", "10YNO-3--------J":"NO3", "10YNO-4--------9":"NO4", "44Y-00000000161I":"FI_EL", "45Y0000000000046":"DK1_CO", "45Y0000000000054":"DK1_DE", "45Y0000000000070":"DK2_KO", "46Y000000000003U":"SE4_SP", "46Y000000000004S":"SE4_NB", "50Y73EMZ34CQL9AJ":"NO2_ND", "50YNBFFTWZRAHA3P":"NO2_DE"}


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
            outFileName = ig107File.replace(".xml", "_extract.csv")
            
        field_order = ["NTC_initial", "CNTC_IVA","NTC_final","AAC","ATC"]
                
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





if __name__=="__main__":
    
    border_order = ["DK1_CO-DK1", "DK1-DK1_CO", "DK1_DE-DK1", "DK1-DK1_DE", "DK1-DK1A", "DK1A-DK1", "DK1-DK2", "DK2-DK1", "DK1A-NO2", "NO2-DK1A", "DK1A-SE3", "SE3-DK1A", "DK2_KO-DK2", "DK2-DK2_KO", "DK2-SE4", "SE4-DK2", "FI_EL-FI", "FI-FI_EL", "FI-SE1", "SE1-FI", "NO1A-NO1", "NO1-NO1A", "NO1A-NO2", "NO2-NO1A", "NO1A-NO5", "NO5-NO1A", "NO1-NO3", "NO3-NO1", "NO1-SE3", "SE3-NO1", "NO2-NO2_ND", "NO2_ND-NO2", "NO2-NO2_DE", "NO2_DE-NO2", "NO2-NO5", "NO5-NO2", "NO3-NO4", "NO4-NO3", "NO3-NO5", "NO5-NO3", "NO3-SE2", "SE2-NO3", "NO4-SE1", "SE1-NO4", "NO4-SE2", "SE2-NO4", "SE3-FI", "FI-SE3", "SE1-SE2", "SE2-SE1", "SE2-SE3", "SE3-SE2", "SE3-SE4", "SE4-SE3", "SE4-SE4_NB", "SE4_NB-SE4", "SE4-SE4_SP", "SE4_SP-SE4"]
    
    try:
        fDir = "..\\data"
        
        files = os.listdir(os.getcwd() + "\\"+ fDir)
        files = [fDir + "\\" + f for f in files if f[-3:] == "xml"]
                
        extract_ig107_files(files, bzb_order = border_order)
        
    except Exception as errMsg:
        print(errMsg)
    
    