#!/usr/bin/python
'''
Script for parsing ig-107 xml file to csv format.

'''
import os
import zipfile
import json
from datetime import datetime
from datetime import timedelta
import xml.etree.ElementTree as ET
# import topology

time_resolution = {"PT60M": timedelta(seconds=3600), "PT15M": timedelta(seconds=900)}
bt2code = {"A25":"CNTC_IVA", "A26":"ATC", "A27":"NTC_final", "A29":"AAC", "B38":"NTC_initial"}
code2bt = {"CNTC_IVA":"A25", "ATC":"A26", "NTC_final":"A27", "AAC":"A29", "NTC_initial":"B38"}

# shortname=[]
# eic=[]
# for t in topology.latest_topology["biddingZones"]:
   # eic.append(t['eic'])
   # shortname.append(t["norCapShortName"])
   
# code2eic={eic[t]:shortname[t] for t in range(len(eic))}


def load_json(fileName:str):
    with open(fileName, 'r') as j:
        obj = json.load(j)
    return obj


def parseTimeSeriesFromCapacityDocument(ig107File, topology):
    # parse all time series defined in implementation guide for ig-107 to a list of dicts.
    ns = {"xmlns":"{urn:iec62325.351:tc57wg16:451-3:capacitydocument:8:0}", "cimp":"{http://www.iec.ch/cimprofile}", "xsi": "{http://www.w3.org/2001/XMLSchema-instance}"}
    tree = ET.parse(ig107File)
    root = tree.getroot()
    time_series = root.findall(ns["xmlns"] + "TimeSeries")
    ts_list = []
    for t in time_series:
        p = t.find(ns["xmlns"] + "Period")
        fromBZ = next(( b['shortName'] for b in topology['biddingZones'] if b['eic'] == t.find(ns["xmlns"] + "out_Domain.mRID").text ), None)
        if fromBZ is None:
            print('From bidding zone not found in topology: ' + t.find(ns["xmlns"] + "out_Domain.mRID").text)

        toBZ =  next(( b['shortName'] for b in topology['biddingZones'] if b['eic'] == t.find(ns["xmlns"] + "in_Domain.mRID").text ), None)
        if toBZ is None:
            print('From bidding zone not found in topology: ' + t.find(ns["xmlns"] + "in_Domain.mRID").text)
            
        border_direction = next((b['name'] for b in topology['biddingZoneBorders'] if b['from']==fromBZ and b['to']==toBZ),None)
        if border_direction is None:
            print("bidding zone border found from " + fromBZ + " to " + toBZ + ".")
        
        ts = {"borderDirection": border_direction, # "borderDirection": code2eic[t.find(ns["xmlns"] + "out_Domain.mRID").text] + "-" + code2eic[t.find(ns["xmlns"] + "in_Domain.mRID").text],
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
    
    

def extract_ig107_files(files, topology, topology_map):
    # For each ig107 file parse content to csv format.
    # If a bzb_order is provided, column order in output csv will follow the bzb_order. Although, if a border does not exist in time series data, it will be skipped in csv parsing.


    outFileNames = []
    
    sep = ';'
    
    for  ig107File in files:
        print("Parsing file: " + ig107File + "...")

        ts = parseTimeSeriesFromCapacityDocument(ig107File, topology)
        ts = [ii for ii in ts if ii["period.resolution"]=="PT60M"]

        timeStamps = list(list(zip(*ts[0]["curve"]))[0])
         
        outFileName = ig107File.replace(".xml", "_extract.csv")
        outFileNames.append(outFileName)   
        
        field_order = ["NTC_initial", "CNTC_IVA","NTC_final","AAC","ATC"]
        
        mapped_borders = [k['key'] for k in topology_map['map']]
        
        print_order = []
        for bzb in topology['biddingZoneBorders']:
            if bzb['name'] in mapped_borders:
                for bt in field_order:
                    match = next(( index for index, item in enumerate(ts) if (item['borderDirection']==bzb['name'] and item['BusinessType']==code2bt[bt]) ), None)
                    if not match is None:
                        print_order.append(match)

        with open(outFileName,"w+") as fid:
            fid.write("MTU"+sep+"Backup"+sep)
            for ii in print_order:
                mapped_border_name = next( (m['value'] for m in topology_map['map'] if m['key'] == ts[ii]['borderDirection']),None)
                fid.write(mapped_border_name + sep)
            fid.write("\n")
            
            fid.write("MTU"+sep+"Backup"+sep)
            
            for ii in print_order:
                fid.write(bt2code[ts[ii]['BusinessType']] + sep)
            fid.write("\n")

            for t in timeStamps:
                backup = False
                fid.write(t.strftime(format="%Y-%m-%dT%H:%MZ")+ sep)
                # TODO check for backup MTUs
                fid.write(str(backup)+sep)
                
                for ii in print_order:
                    c = ts[ii]['curve']
                    q = next((point[1] for point in c if point[0]==t), None)
                    fid.write('{:.1f}'.format(q) + sep)
                
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
    
    path_to_topology = '..\\topology\\data\\'
    topology_file_name = 'topology_ig107.JSON'
    
    topology_map_name = 'map_ig107-intradayNTC.JSON'
    
    fDir = "..\\data\\2024w13\\"
        
    ntc_topology = load_json(path_to_topology + topology_file_name)
    topology_map = load_json(path_to_topology + topology_map_name)
    
    
    # ordering biddingzone borders in pair-wise opposite border directions:
    bzbs = ntc_topology['biddingZoneBorders'].copy()
    border_directions = []
    for ii, bzb in enumerate(bzbs):
        if not ii in border_directions:
            border_directions.append(ii)
        reverse_border = next(( reverse_ii for reverse_ii, b in enumerate(ntc_topology['biddingZoneBorders']) if (b['from']==bzb['to'] and b['to']==bzb['from'] and b['type']==bzb['type'] and not reverse_ii in border_directions)), None)
        if not reverse_border is None:
            border_directions.append(reverse_border)

    ntc_topology['biddingZoneBorders']  = [ntc_topology['biddingZoneBorders'][jj] for jj in border_directions]
    
    
    try:
        files = os.listdir(os.getcwd() + "\\"+ fDir)
        files = [fDir + "\\" + f for f in files if f[-3:] == "xml"]
                
        # public_files = extract_ig107_files(files, bzb_order = border_order)
        # zipFiles(fDir.split('\\')[-1]+'_public.zip', fDir, public_files)
                
        private_files = extract_ig107_files(files, ntc_topology, topology_map)
        zipFiles(fDir.split('\\')[-1]+'xyz.zip', fDir, private_files, deleteUnzipped=False)
        
    except Exception as errMsg:
        print(errMsg)
    
    
