from lxml import etree
import pandas as pd
import datetime,time
import multiprocessing
import numpy as np

file_path='C:/Users/benson/Desktop/2016/2015-xml/Realtime Constrained Totals Report/PUB_RealtimeConstTotals_2017010101_v12.xml'

def get_DataFrame_RealtimeConstrainedTotal(filePath):
    doc = etree.parse(filePath)
    root = doc.getroot()
    html = etree.HTML(etree.tostring(root))
    data_list = []
    dict = {}
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat = html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    deliverydate = html.xpath('//DocBody/DeliveryDate'.lower())
    dict['DeliveryDate'] = deliverydate[0].text
    deliverydate = html.xpath('//DocBody/DeliveryHour'.lower())
    dict['DeliveryHour'] = deliverydate[0].text
    IntervalEnergy=html.xpath('//Energies/IntervalEnergy'.lower())
    for energy in IntervalEnergy:
        energy_node = etree.HTML(etree.tostring(energy))
        dict['Interval']=energy_node.xpath('//Interval/text()'.lower())[0]
        MQ=energy_node.xpath('//MQ'.lower())
        for mq in MQ:
            mq_node=etree.HTML(etree.tostring(mq))
            dict['MarketQuantity'] = mq_node.xpath('//MarketQuantity/text()'.lower())[0]
            dict['EnergyMW'] = mq_node.xpath('//EnergyMW/text()'.lower())[0]
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1) + '-' + str((int(dict['Interval']) - 1) * 5)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H-%M')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

xml_folder = '/home/peak/Dropbox (Peak Power Inc)/IESO/IESO_Organized/2016/System Adequacy/'
csv_folder = 'C:/Users/benson/Desktop/2015-csv/Realtime Shadow Prices/'

def generate_list_RealtimeConstrainedTotal(startHour,endHour,folder):
    hourList=pd.date_range(startHour,endHour,freq='H')
    # print hourList
    fileList=[]
    # PUB_RealtimeConstTotals_2017010101_v12
    for hour in hourList:
        month_str=str(hour).split(' ')[0].split('-')
        hourStr=str(hour).split(' ')[1].split(':')[0]
        hourInt=int(hourStr)+1
        if hourInt<10:
            fileList.append('%sPUB_RealtimeConstTotals_%s%s%s0%i_v1.xml' % (folder, month_str[0], month_str[1], month_str[2], hourInt))
        else:
            fileList.append(
                '%sPUB_RealtimeConstTotals_%s%s%s%i_v1.xml' % (folder, month_str[0], month_str[1], month_str[2], hourInt))
    return fileList

def IsSubString(subList,str):
    flag=True
    for subStr in subList:
        if not(subStr in str):
            flag=False
    return flag
def save_csv_RealtimeConstrainedTotal(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_RealtimeConstrainedTotal(filename)
    print df.shape
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (csv_folder, hour), header=True)
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

def year_xml2df_RealtimeConstrainedTotal(xml_folder):
    gen_file_list=generate_list_RealtimeConstrainedTotal('2016-01-01 00:00','2016-12-31 23:00',xml_folder)
    get_list_file=get_list_filename(xml_folder,['.xml'])
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
    t1=datetime.datetime.now()
    print t1
    pool=multiprocessing.Pool(multiprocessing.cpu_count())
    pool.map(save_csv_RealtimeConstrainedTotal,used_list)
    t2=datetime.datetime.now()
    print t2-t1

year_xml2df_RealtimeConstrainedTotal(xml_folder)