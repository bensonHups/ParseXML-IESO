from lxml import etree
import pandas as pd
import datetime,time
import numpy as np
file_path='C:/Users/benson/Desktop/2016/PUB_GlobalAdjustment_201601_v1.xml'

def get_DataFrame_GlobalAdjustment(filePath):
    doc = etree.parse(filePath)
    root = doc.getroot()
    html = etree.HTML(etree.tostring(root))
    data_list = []
    dict = {}
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat = html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    TradeMonth = html.xpath('//DocBody/TradeMonth'.lower())
    dict['TradeMonth'] = TradeMonth[0].text
    FirstEstimateRate=html.xpath('//DocBody/GAValues/FirstEstimateRate'.lower())
    dict['FirstEstimateRate'] = FirstEstimateRate[0].text
    timestr = dict['TradeMonth']
    dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m')
    dict2 = {}
    dict2.update(dict)
    data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

file_folder='C:/Users/benson/Desktop/2016/Global Adjustment Report/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Global Adjustment Report/'
# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

#generate all the fileList
def generate_list_GlobalAdjustment(startMonth,endMonth,folder):
    monthList=pd.date_range(startMonth,endMonth,freq='BM')
    # print monthList
    monthListStr=[]
    # PUB_GlobalAdjustment_201601_v1
    for month in monthList:
        month_str=str(month).split(' ')[0].split('-')
        for i in np.arange(1,4,1):
            monthListStr.append('%sPUB_GlobalAdjustment_%s%s_v%i.xml' % (folder, month_str[0], month_str[1], i))
    return monthListStr

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

def save_csv_GlobalAdjustment(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_GlobalAdjustment(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (save_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)

gen_file_list=generate_list_GlobalAdjustment('2016-01','2017-01',file_folder)
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
for i in range(len(used_list)):
    save_csv_GlobalAdjustment(used_list[i])

# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_PredispAreaOpResShortfalls,used_list)
# t2=datetime.datetime.now()
# print t2-t1