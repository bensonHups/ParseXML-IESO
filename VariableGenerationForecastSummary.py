from lxml import etree
import pandas as pd
import datetime,time
import numpy as np
import  multiprocessing

file_path='C:/Users/benson/Desktop/2016-example/PUB_VGForecastSummary_20160201_v22.xml'
def get_DataFrame_VGForecastSummary(filePath):
    doc = etree.parse(filePath)
    root= doc.getroot()
    html = etree.HTML(etree.tostring(root))
    data_list=[]
    dict = {}
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat = html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    dict['ForecastTimeStamp']=html.xpath('//DocBody/ForecastTimeStamp/text()'.lower())[0]
    OrganizationData=html.xpath('//DocBody/OrganizationData'.lower())
    for organization in OrganizationData:
        organization_node = etree.HTML(etree.tostring(organization))
        dict['OrganizationType']=organization_node.xpath('//OrganizationType/text()'.lower())[0]
        FuelData=organization_node.xpath('//FuelData'.lower())
        for fuel in FuelData:
            fuel_node = etree.HTML(etree.tostring(fuel))
            dict['FuelType']=fuel_node.xpath('//FuelType/text()'.lower())[0]
            ResourceData=fuel_node.xpath('//ResourceData'.lower())
            for resource in ResourceData:
                resource_node = etree.HTML(etree.tostring(resource))
                dict['ZoneName'] = resource_node.xpath('//ZoneName/text()'.lower())[0]
                EnergyForecast= resource_node.xpath('//EnergyForecast'.lower())
                for energy in EnergyForecast:
                    energy_node = etree.HTML(etree.tostring(energy))
                    dict['ForecastDate']=energy_node.xpath('//ForecastDate/text()'.lower())[0]
                    ForecastInterval= resource_node.xpath('//ForecastInterval'.lower())
                    for interval in ForecastInterval:
                        interval_node = etree.HTML(etree.tostring(interval))
                        dict['ForecastHour'] = interval_node.xpath('//ForecastHour/text()'.lower())[0]
                        dict['MWOutput'] = interval_node.xpath('//MWOutput/text()'.lower())[0]
                        timeStr = dict['ForecastDate'] + '-' + str(int(dict['ForecastHour']) - 1)
                        dict['datetime'] = datetime.datetime.strptime(timeStr, '%Y-%m-%d-%H')
                        dict2 = {}
                        dict2.update(dict)
                        data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

file_folder='C:/Users/benson/Desktop/2016/Variable Generation Forecast Summary Report/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Variable Generation Forecast Summary Report/'
# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

#generate all the fileList
def generate_list_VGForecastSummary(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]
    # PUB_VGForecastSummary_20160201_v24
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        for i in np.arange(1,25,1):
            dayListStr.append('%sPUB_VGForecastSummary_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
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

def save_csv_VGForecastSummary(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_VGForecastSummary(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (save_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)

gen_file_list=generate_list_VGForecastSummary('20160101','20161231',file_folder)
get_list_file=get_list_filename(file_folder,['.xml'])
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
# for i in range(len(used_list)):
#     save_csv_DayAheadConstrained(used_list[i])
save_csv_VGForecastSummary(used_list[0])
# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_PredispatchConstrained,used_list)
# t2=datetime.datetime.now()
# print t2-t1
