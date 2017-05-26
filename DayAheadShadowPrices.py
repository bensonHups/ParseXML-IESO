from lxml import etree
import pandas as pd
import datetime,time
import multiprocessing
import numpy as np

file_path='C:/Users/benson/Desktop/2016-example/PUB_DAShadowPrices_20160101_v1.xml'

def get_DataFrame_DAShadowPrices(filePath):
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
    NodePrices= html.xpath('//DocBody/NodePrices'.lower())
    for node_price in NodePrices:
        node=etree.HTML(etree.tostring(node_price))
        dict['NodeName']=node.xpath('//nodename/text()')[0]
        HourlyPrices=node.xpath('//HourlyPrices'.lower())
        for hour_price in HourlyPrices:
            hourNode=etree.HTML(etree.tostring(hour_price))
            dict['DeliveryHour']=hourNode.xpath('//DeliveryHour/text()'.lower())[0]
            Prices=hourNode.xpath('//Prices'.lower())
            for price in Prices:
                priceNode=etree.HTML(etree.tostring(price))
                dict['PriceType'] = priceNode.xpath('//PriceType/text()'.lower())[0]
                dict['MCP'] = priceNode.xpath('//MCP/text()'.lower())[0]
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

file_folder='C:/Users/benson/Desktop/2016/Day-Ahead Shadow Prices Report/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Day-Ahead Shadow Prices Report/'

def generate_list_DAShadowPrices(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]
    # PUB_DAShadowPrices_20160102_v2
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        for i in np.arange(1,3,1):
            dayListStr.append('%sPUB_DAShadowPrices_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
    return dayListStr
def IsSubString(subList,str):
    flag=True
    for subStr in subList:
        if not(subStr in str):
            flag=False
    return flag

def save_csv_DAShadowPrices(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_DAShadowPrices(filename)
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

gen_file_list=generate_list_DAShadowPrices('20160101','20161231',file_folder)
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
print '-----------------%i-------------------------'%len(used_list)
# for i in range(len(used_list)):
#     save_csv_DispAreaOpResAndEnergyCalled(used_list[i])
save_csv_DAShadowPrices(used_list[0])
# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_TRAPreauctionInterfaceHistoryMonthly(),used_list)
# t2=datetime.datetime.now()
# print t2-t1