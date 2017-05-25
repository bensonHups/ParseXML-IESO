from lxml import etree
import pandas as pd
import datetime,time
import multiprocessing
import numpy as np
# file_path='C:/Users/benson/Desktop/2016/PUB_PredispMktPrice_20160101_v1.xml'
file_path='C:/Users/benson/Downloads/Predispatch Market Price Report.xml'
# PUB_PredispShadowPrices_20160101_v1
def get_DataFrame_PredispMktPrice(filePath):
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
    IntertieZonalPrices = html.xpath('//DocBody/IntertieZonalPrices'.lower())
    for zoneprice in IntertieZonalPrices:
        zoneprice_node=etree.HTML(etree.tostring(zoneprice))
        dict['IntertieZoneName'] =zoneprice_node.xpath('//IntertieZoneName/text()'.lower())[0]
        Prices=zoneprice_node.xpath('//Prices'.lower())
        for price in Prices:
            price_node = etree.HTML(etree.tostring(price))
            dict['PriceType'] =price_node.xpath('//PriceType/text()'.lower())[0]
            HourlyPrice=price_node.xpath('//HourlyPrice'.lower())
            for hour in HourlyPrice:
                hour_node=etree.HTML(etree.tostring(hour))
                dict['DeliveryHour']=hour_node.xpath('//DeliveryHour/text()'.lower())[0]
                dict['MCP']=hour_node.xpath('//MCP/text()'.lower())[0]

    # for i in range(len(IntertieZonalPrices)):
    #     IntertieZoneName=html.xpath(((path0+'/IntertieZoneName')%(i+1)).lower())
    #     dict['IntertieZoneName']=IntertieZoneName[0].text
    #     Prices=html.xpath(((path0+'/Prices')%(i+1)).lower())
    #     path1=path0+'/Prices[%i]'
    #     for j in range(len(Prices)):
    #         PriceType=html.xpath(((path1+'/PriceType')%(i+1,j+1)).lower())
    #         dict['PriceType'] = PriceType[0].text
    #         HourlyPrice=html.xpath(((path1+'/HourlyPrice')%(i+1,j+1)).lower())
    #         path2=path1+'/HourlyPrice[%i]'
    #         for k in range(len(HourlyPrice)):
    #             DeliveryHour=html.xpath(((path2+'/DeliveryHour')%(i+1,j+1,k+1)).lower())
    #             dict['DeliveryHour'] = DeliveryHour[0].text
    #             MCP = html.xpath(((path2 + '/MCP') % (i + 1, j + 1, k + 1)).lower())
    #             dict['MCP'] = MCP[0].text
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)



file_folder='C:/Users/benson/Desktop/2016/Predispatch Market Price Report/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Predispatch Market Price Report/'
# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

#generate all the fileList
def generate_list_PredispMktPrice(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]

    # PUB_PredispMktPrice_20160101_v1
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        for i in np.arange(1,33,1):
            dayListStr.append('%sPUB_PredispMktPrice_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
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

def save_csv_PredispMktPrice(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_PredispMktPrice(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (save_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)

gen_file_list=generate_list_PredispMktPrice('20160101','20161231',file_folder)
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
save_csv_PredispMktPrice(used_list[0])

# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_PredispMktPrice,used_list)
# t2=datetime.datetime.now()
# print t2-t1