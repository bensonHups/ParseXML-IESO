from lxml import etree
import pandas as pd
import datetime,time
import numpy as np
import multiprocessing
file_path='C:/Users/benson/Desktop/2016-example/PUB_DAConstTotals_20111013_v1.xml'
def get_DataFrame_DayAheadConstrained(filePath):
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
    dict['deliverydate'] = deliverydate[0].text
    HourlyConstrainedEnergy=html.xpath('//HourlyConstrainedEnergy'.lower())
    for hour in HourlyConstrainedEnergy:
        hour_node=etree.HTML(etree.tostring(hour))
        dict['DeliveryHour']=hour_node.xpath('//DeliveryHour/text()'.lower())[0]
        MQ=hour_node.xpath('//MQ'.lower())
        for mq in MQ:
            mq_node=etree.HTML(etree.tostring(mq))
            dict['MarketQuantity'] = mq_node.xpath('//MarketQuantity/text()'.lower())[0]
            dict['EnergyMW'] = mq_node.xpath('//EnergyMW/text()'.lower())[0]
            timestr = dict['deliverydate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)



file_folder='C:/Users/benson/Desktop/2016/Day-Ahead Constrained Total Report/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Day-Ahead Constrained Total Report/'
# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

#generate all the fileList
def generate_list_DayAheadConstrained(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]
    # PUB_DAConstTotals_20160827_v2
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        for i in np.arange(1,3,1):
            dayListStr.append('%sPUB_DAConstTotals_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
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

def save_csv_DayAheadConstrained(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_DayAheadConstrained(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (save_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)

def process_year_xml2df(file_folder):
    gen_file_list=generate_list_DayAheadConstrained('20160101','20161231',file_folder)
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
# save_csv_DayAheadConstrained(used_list[0])
    t1=datetime.datetime.now()
    print t1
    pool=multiprocessing.Pool(multiprocessing.cpu_count())
    pool.map(save_csv_DayAheadConstrained,used_list)
    t2=datetime.datetime.now()
    print t2-t1

test_integrate_folder='/home/peak/IESO-CSV/2016/System Adequacy'
test_integrate_file='C:/Users/benson/Desktop/2016-example/PUB_Adequacy_20160101_v1'

# PUB_Adequacy_20160103_v7
def generate_day_table(dirpath):
    df=pd.read_csv(test_integrate_file,header=True)




