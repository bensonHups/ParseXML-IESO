from lxml import etree
import pandas as pd
import datetime,time
import numpy as np
import multiprocessing

# file_path='C:/Users/benson/Desktop/2016/PUB_PredispAreaOpResShortfalls_20160101_v1.xml'
file_path='C:/Users/benson/Downloads/Predispatch Area Operating Reserve Shortfalls.xml'

def get_DataFrame_PredispAreaOpResShortfalls(filePath):
    doc = etree.parse(filePath)
    root = doc.getroot()
    html = etree.HTML(etree.tostring(root))
    html_Str=etree.tostring(root)
    area_list=html_Str.split('<Area>')
    area_list_str=[]
    for area in area_list:
        if area.find('</Area>')>0:
            cut_list=area.split('</Area>')
            area_list_str.append(cut_list[0])
    print len(area_list_str)
    data_list = []
    dict = {}
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat = html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    deliverydate = html.xpath('//DocBody/DeliveryDate'.lower())
    dict['DeliveryDate'] = deliverydate[0].text
    AreaEnergies= html.xpath('//DocBody/AreaEnergies'.lower())
    path0 = '//DocBody/AreaEnergies[%i]'
    AreaEnergies = html.xpath ('//AreaEnergies//Area'.lower())
    for i in range(len(AreaEnergies)):
        # Area=html.xpath(((path0+'/Area')%(i+1)).lower())
        # dict['Area'] = Area[0].text
        # print Area[0].text @!!!!! I don't know why it doesn't work
        dict['Area'] = area_list_str[0]
        HourlyEnergies=html.xpath(((path0+'/HourlyEnergies')%(i+1)).lower())
        path1=path0+'/HourlyEnergies[%i]'
        for j in range(len(HourlyEnergies)):
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            MinORRequired = html.xpath(((path1 + '/MinORRequired') % (i + 1, j + 1)).lower())
            dict['MinORRequired'] = MinORRequired[0].text
            Scheduled10S = html.xpath(((path1 + '/Scheduled10S') % (i + 1, j + 1)).lower())
            dict['Scheduled10S'] = Scheduled10S[0].text
            Scheduled10N = html.xpath(((path1 + '/Scheduled10N') % (i + 1, j + 1)).lower())
            dict['Scheduled10N'] = Scheduled10N[0].text
            ORShortfall = html.xpath(((path1 + '/ORShortfall') % (i + 1, j + 1)).lower())
            dict['ORShortfall'] = ORShortfall[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

file_folder='C:/Users/benson/Desktop/2016/Predispatch Area Operating Reserve Shortfalls/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Predispatch Area Operating Reserve Shortfalls/'
# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

#generate all the fileList
def generate_list_PredispAreaOpResShortfalls(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]

    # PUB_PredispAreaOpResShortfalls_20160101_v32
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        for i in np.arange(1,33,1):
            dayListStr.append('%sPUB_PredispAreaOpResShortfalls_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
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

def save_csv_PredispAreaOpResShortfalls(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_PredispAreaOpResShortfalls(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (save_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)

gen_file_list=generate_list_PredispAreaOpResShortfalls('20160101','20161231',file_folder)
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
save_csv_PredispAreaOpResShortfalls(used_list[0])

# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_PredispAreaOpResShortfalls,used_list)
# t2=datetime.datetime.now()
# print t2-t1
