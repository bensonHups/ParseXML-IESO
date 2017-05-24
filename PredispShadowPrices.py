from lxml import etree
import pandas as pd
import datetime,time
import multiprocessing
import numpy as np

# file_path='C:/Users/benson/Desktop/2016/PUB_PredispShadowPrices_20160101_v1.xml'
file_path='C:/Users/benson/Downloads/Predispatch Shadow Prices Report.xml'
# PUB_PredispShadowPrices_20160101_v1
def get_DataFrame_PredispShadowPrices(filePath):
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



file_folder='C:/Users/benson/Desktop/2016/Predispatch Shadow Prices Report/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Predispatch Shadow Prices Report/'
# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

# benson writed the code here
#generate all the fileList
def generate_list_filename(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]
    # excep = ['2016021304', '2016081012', '2016081013', '2016081924', '2016082424']
    # PUB_PredispShadowPrices_20160101_v1
    # PUB_PredispShadowPrices_20160101_v32
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        for i in np.arange(1,33,1):
            dayListStr.append('%sPUB_PredispShadowPrices_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
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

def parse_dayandsave(day):
    file_list=generate_list_filename(day,day,file_folder)
    for i in np.arange(0,len(file_list),1):
        df=get_DataFrame_PredispShadowPrices(file_list[i])
        df.to_csv('%s%s-%i.csv' % (save_folder, day,i), header=True)
        print "saved:%s"%file_list[i]
        time.sleep(2)
    return day

def save_csv(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_PredispShadowPrices(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (save_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)

gen_file_list=generate_list_filename('20160101','20161231',file_folder)
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
print '------------------------------------------'


t1=datetime.datetime.now()
print t1
pool=multiprocessing.Pool(multiprocessing.cpu_count())
pool.map(save_csv,used_list)
t2=datetime.datetime.now()
print t2-t1
