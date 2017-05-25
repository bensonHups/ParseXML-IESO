from lxml import etree
import pandas as pd
import datetime,time
import numpy as np
import multiprocessing
# file_path='C:/Users/benson/Desktop/2016/PUB_OntarioZonalDemand_20160621_v1.xml'
file_path='C:/Users/benson/Downloads/Ontario Zonal Demand Forecast Report.xml'
def get_DataFrame_OntarioZonalDemand(filePath):
    doc = etree.parse(filePath)
    root = doc.getroot()
    html = etree.HTML(etree.tostring(root))
    data_list = []
    dict = {}
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat = html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    dict['DemandForecastStartDate']=html.xpath('//DemandForecastStartDate/text()'.lower())[0]
    dict['DemandForecastEndDate'] = html.xpath('//DemandForecastEndDate/text()'.lower())[0]
    ZonalDemands = html.xpath('//DocBody/ZonalDemands'.lower())
    for zone in ZonalDemands:
        zone_node=etree.HTML(etree.tostring(zone))
        dict['DeliveryDate']=zone_node.xpath('//DeliveryDate/text()'.lower())[0]
        Ontario_Demands=zone_node.xpath('//Ontario/Demand'.lower())
        for odemand in Ontario_Demands:
            odemand_node = etree.HTML(etree.tostring(odemand))
            dict['zone']='Ontario'
            dict['DeliveryHour'] = odemand_node.xpath('//DeliveryHour/text()'.lower())[0]
            dict['EnergyMW'] = odemand_node.xpath('//EnergyMW/text()'.lower())[0]
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        East_Demands = zone_node.xpath('//East/Demand'.lower())
        for edemand in East_Demands:
            edemand_node = etree.HTML(etree.tostring(edemand))
            dict['zone']='East'
            dict['DeliveryHour'] = edemand_node.xpath('//DeliveryHour/text()'.lower())[0]
            dict['EnergyMW'] = edemand_node.xpath('//EnergyMW/text()'.lower())[0]
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        West_Demands = zone_node.xpath('//West/Demand'.lower())
        for wdemand in West_Demands:
            wdemand_node = etree.HTML(etree.tostring(wdemand))
            dict['zone'] = 'West'
            dict['DeliveryHour'] = wdemand_node.xpath('//DeliveryHour/text()'.lower())[0]
            dict['EnergyMW'] = wdemand_node.xpath('//EnergyMW/text()'.lower())[0]
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)


    # for i in range(len(ZonalDemands)):
    #     DeliveryDate=html.xpath(((path0+'/DeliveryDate')%(i+1)).lower())
    #     dict['DeliveryDate']=DeliveryDate[0].text
    #     dict['zone']='Ontario'
    #     Demand=html.xpath(((path0 + '/ZonalDemand/Ontario/Demand') % (i + 1)).lower())
    #     path1_1=path0 + '/ZonalDemand/Ontario/Demand[%i]'
    #     for j in range(len(Demand)):
    #         DeliveryHour=html.xpath(((path1_1+'/DeliveryHour')%(i+1,j+1)).lower())
    #         dict['DeliveryHour'] = DeliveryHour[0].text
    #         EnergyMW=html.xpath(((path1_1+'/EnergyMW')%(i+1,j+1)).lower())
    #         dict['EnergyMW'] = EnergyMW[0].text
    #         timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
    #         dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
    #         dict2 = {}
    #         dict2.update(dict)
    #         data_list.append(dict2)
    # for i in range(len(ZonalDemands)):
    #     DeliveryDate = html.xpath(((path0 + '/DeliveryDate') % (i + 1)).lower())
    #     dict['DeliveryDate'] = DeliveryDate[0].text
    #     dict['zone'] = 'East'
    #     Demand = html.xpath(((path0 + '/ZonalDemand/East/Demand') % (i + 1)).lower())
    #     path1_1 = path0 + '/ZonalDemand/East/Demand[%i]'
    #     for j in range(len(Demand)):
    #         DeliveryHour = html.xpath(((path1_1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
    #         dict['DeliveryHour'] = DeliveryHour[0].text
    #         EnergyMW = html.xpath(((path1_1 + '/EnergyMW') % (i + 1, j + 1)).lower())
    #         dict['EnergyMW'] = EnergyMW[0].text
    #         timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
    #         dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
    #         dict2 = {}
    #         dict2.update(dict)
    #         data_list.append(dict2)
    # for i in range(len(ZonalDemands)):
    #     DeliveryDate = html.xpath(((path0 + '/DeliveryDate') % (i + 1)).lower())
    #     dict['DeliveryDate'] = DeliveryDate[0].text
    #     dict['zone'] = 'West'
    #     Demand = html.xpath(((path0 + '/ZonalDemand/West/Demand') % (i + 1)).lower())
    #     path1_1 = path0 + '/ZonalDemand/West/Demand[%i]'
    #     for j in range(len(Demand)):
    #         DeliveryHour = html.xpath(((path1_1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
    #         dict['DeliveryHour'] = DeliveryHour[0].text
    #         EnergyMW = html.xpath(((path1_1 + '/EnergyMW') % (i + 1, j + 1)).lower())
    #         dict['EnergyMW'] = EnergyMW[0].text
    #         timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
    #         dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
    #         dict2 = {}
    #         dict2.update(dict)
    #         data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)


file_folder='C:/Users/benson/Desktop/2016/Ontario Zonal Demand Forecast/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Ontario Zonal Demand Forecast/'
# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

#generate all the fileList
def generate_list_OntarioZonalDemand(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]

    # PUB_OntarioZonalDemand_20160621_v1
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        dayListStr.append('%sPUB_OntarioZonalDemand_%s%s%s_v1.xml' % (folder, daystr[0], daystr[1], daystr[2]))
    return dayListStr

def IsSubString(subList,str):
    flag=True
    for subStr in subList:
        if not(subStr in str):
            flag=False
    return flag
#get all the files name from folder,filter by FlagStr
def get_list_filename(file_folder,FlagStr=[]):
    import os
    fileList=[]
    fileNames=os.listdir(file_folder)
    if len(fileNames)>0:
        for file in fileNames:
            if len(FlagStr)>0:
                if IsSubString(FlagStr,file):
                    fileList.append(os.path.join(file_folder,file))
            else:
                fileList.append(os.path.join(file_folder, file))
    if len(fileList)>0:
        fileList.sort()
    return fileList

def save_csv_OntarioZonalDemand(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_OntarioZonalDemand(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (save_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)

gen_file_list=generate_list_OntarioZonalDemand('20160621','20161231',file_folder)
get_list_file=get_list_filename(file_folder,['.xml'])
print len(get_list_file)
used_list=[]
for gen_str in gen_file_list:
    flag=True
    for get_str in get_list_file:
        if gen_str==get_str:
            flag=False
    if flag==False:
        used_list.append(gen_str)
    if flag:
        print gen_str
print '--------------------------len:%i----------------'%len(used_list)
save_csv_OntarioZonalDemand(used_list[0])

# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_PredispAreaOpResShortfalls,used_list)
# t2=datetime.datetime.now()
# print t2-t1