from lxml import etree
import pandas as pd
import datetime,time
import numpy as np
import  multiprocessing

file_path='C:/Users/benson/Desktop/2016-example/PUB_PredispMktTotals_20160101_v1.xml'
def get_DataFrame_PredispatchMarket(filePath):
    doc = etree.parse(filePath)
    root= doc.getroot()
    html = etree.HTML(etree.tostring(root))
    data_list=[]
    dict = {}
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat = html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    deliverydate = html.xpath('//DocBody/DeliveryDate'.lower())
    dict['DeliveryDate'] = deliverydate[0].text
    HourlyUnConstrainedEnergy=html.xpath('//Energies/HourlyUnConstrainedEnergy'.lower())
    for energy in HourlyUnConstrainedEnergy:
        node = etree.HTML(etree.tostring(energy))
        dict['DeliveryHour']=node.xpath('//DeliveryHour/text()'.lower())[0]
        MQ=node.xpath('//MQ'.lower())
        for mq in MQ:
            mq_node=etree.HTML(etree.tostring(mq))
            dict['MarketQuantity']=mq_node.xpath('//MarketQuantity/text()'.lower())[0]
            dict['EnergyMW'] = mq_node.xpath('//EnergyMW/text()'.lower())[0]
    # for i in range(len(IntervalEnergy)):
    #     Interval=html.xpath(('//Energies/IntervalEnergy[%i]/Interval'%(i+1)).lower())
    #     dict['Interval'] = Interval[0].text
    #     MQ=html.xpath(('//Energies/IntervalEnergy[%i]/MQ/MarketQuantity'%(i+1)).lower())
    #     for j in range(len(MQ)):
    #         MarketQuantity=html.xpath(('//Energies/IntervalEnergy[%i]/MQ[%i]/MarketQuantity'%(i+1,j+1)).lower())
    #         dict['MarketQuantity']=MarketQuantity[0].text
    #         EnergyMW=html.xpath(('//Energies/IntervalEnergy[%i]/MQ[%i]/EnergyMW'%(i+1,j+1)).lower())
    #         dict['EnergyMW'] = EnergyMW[0].text
            timeStr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timeStr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)


file_folder='C:/Users/benson/Desktop/2016/Predispatch Market Totals Report/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Predispatch Market Totals Report/'
# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

#generate all the fileList
def generate_list_PredispatchMarket(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]
    # PUB_PredispMktTotals_20160101_v12
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        for i in np.arange(1,33,1):
            dayListStr.append('%sPUB_PredispMktTotals_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
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

def save_csv_PredispatchMarket(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_PredispatchMarket(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (save_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)

gen_file_list=generate_list_PredispatchMarket('20160101','20161231',file_folder)
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
save_csv_PredispatchMarket(used_list[0])
# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_PredispatchConstrained,used_list)
# t2=datetime.datetime.now()
# print t2-t1
