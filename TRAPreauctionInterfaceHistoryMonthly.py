from lxml import etree
import pandas as pd
import datetime,time
import numpy as np
import multiprocessing

file_path='C:/Users/benson/Desktop/2016/PUB_TRAPreauctionInterfaceHistoryMonthly_201601_v1.xml'
def get_node_value(node):
    if len(node)>0:
        return node[0].text
    else:
        return  -1
def get_DataFrame_TRAPreauctionInterfaceHistoryMonthly(filePath):
    doc = etree.parse(filePath)
    root= doc.getroot()
    html = etree.HTML(etree.tostring(root))
    data_list=[]
    dict = {}
    #HEADER
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat= html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    formonth = html.xpath('//docbody/formonth')
    dict['ForMonth'] = formonth[0].text
    deliverydate= html.xpath('//docbody/IntertieFlowsAndCapabilitiesByDate'.lower())
    path0='//docbody/IntertieFlowsAndCapabilitiesByDate[%i]'
    for i in range(len(deliverydate)):
        DeliveryDate=html.xpath(((path0+'/DeliveryDate')%(i+1)).lower())
        dict['DeliveryDate']=get_node_value(DeliveryDate)
        IntertieFlowsAndCapabilities= html.xpath(((path0+'/IntertieFlowsAndCapabilities')%(i+1)).lower())
        path1=path0+'/IntertieFlowsAndCapabilities[%i]'
        for j in range(len(IntertieFlowsAndCapabilities)):
            IntertieZoneName=html.xpath(((path1+'/IntertieZoneName')%(i+1,j+1)).lower())
            dict['IntertieZoneName'] = get_node_value(IntertieZoneName)
            HourlyFlowsAndCapabilities=html.xpath(((path1+'/HourlyFlowsAndCapabilities')%(i+1,j+1)).lower())
            path2=path1+'/HourlyFlowsAndCapabilities[%i]'
            for k in range(len(HourlyFlowsAndCapabilities)):
                DeliveryHour=html.xpath(((path2+'/DeliveryHour')%(i+1,j+1,k+1)).lower())
                dict['DeliveryHour'] = get_node_value(DeliveryHour)
                ScheduledFlow = html.xpath(((path2 + '/ScheduledFlow') % (i + 1, j + 1, k + 1)).lower())
                dict['ScheduledFlow'] = get_node_value(ScheduledFlow)
                ActualFlow = html.xpath(((path2 + '/ActualFlow') % (i + 1, j + 1, k + 1)).lower())
                dict['ActualFlow'] = get_node_value(ActualFlow)
                IntertieTransferCapabilityIn = html.xpath(((path2 + '/IntertieTransferCapabilityIn') % (i + 1, j + 1, k + 1)).lower())
                dict['IntertieTransferCapabilityIn'] = get_node_value(IntertieTransferCapabilityIn)
                IntertieTransferCapabilityOut = html.xpath(((path2 + '/IntertieTransferCapabilityOut') % (i + 1, j + 1, k + 1)).lower())
                if dict['DeliveryHour']<0:
                    print 'DeliveryHour error!!'
                else:
                    dict['IntertieTransferCapabilityOut'] = get_node_value(IntertieTransferCapabilityOut)
                    timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                    dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                    dict2 = {}
                    dict2.update(dict)
                    data_list.append(dict2)

    return pd.DataFrame.from_dict(data_list)

file_folder='C:/Users/benson/Desktop/2016/Monthly Historical Interface Flows-Schedules- and Transmission Transfer Capability Realtime Intertie Scheduling Limits/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Monthly Historical Interface Flows-Schedules- and Transmission Transfer Capability Realtime Intertie Scheduling Limits/'

def generate_list_TRAPreauctionInterfaceHistoryMonthly(startMonth,endMonth,folder):
    monthList=pd.date_range(startMonth,endMonth,freq='BM')
    # print monthList
    monthListStr=[]
    # PUB_TRAPreauctionInterfaceHistoryMonthly_201601_v1
    for month in monthList:
        month_str=str(month).split(' ')[0].split('-')
        for i in np.arange(1,31,1):
            monthListStr.append('%sPUB_TRAPreauctionInterfaceHistoryMonthly_%s%s_v%i.xml' % (folder, month_str[0], month_str[1], i))
    return monthListStr
def IsSubString(subList,str):
    flag=True
    for subStr in subList:
        if not(subStr in str):
            flag=False
    return flag

def save_csv_TRAPreauctionInterfaceHistoryMonthly(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_TRAPreauctionInterfaceHistoryMonthly(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (save_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

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

gen_file_list=generate_list_TRAPreauctionInterfaceHistoryMonthly('2016-01','2017-04',file_folder)
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
    # if flag:
        # print gen_str
print '-----------------%i-------------------------'%len(used_list)
for i in range(len(used_list)):
    save_csv_TRAPreauctionInterfaceHistoryMonthly(used_list[i])
# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_TRAPreauctionInterfaceHistoryMonthly(),used_list)
# t2=datetime.datetime.now()
# print t2-t1