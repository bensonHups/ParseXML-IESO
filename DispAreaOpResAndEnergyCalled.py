from lxml import etree
import pandas as pd
import datetime,time
import numpy as np
import  multiprocessing

file_path='C:/Users/benson/Desktop/2016-example/PUB_DispAreaOpResAndEnergyCalled_2016010101_v1.xml'
def get_DataFrame_DispAreaOpResAndEnergyCalled(filePath):
    doc = etree.parse(filePath)
    root= doc.getroot()
    html_Str = etree.tostring(root)
    html_Str = html_Str.replace('<Area>', '<PArea>')
    html_Str = html_Str.replace('</Area>', '</PArea>')
    html = etree.HTML(html_Str)
    data_list=[]
    dict = {}
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat = html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    dict['DeliveryDate'] = html.xpath('//DocBody/DeliveryDate/text()'.lower())[0]
    dict['DeliveryHour'] = html.xpath('//DocBody/DeliveryHour/text()'.lower())[0]
    AreaEnergies=html.xpath('//DocBody/AreaEnergies'.lower())
    for energy in AreaEnergies:
        energy_node=etree.HTML(etree.tostring(energy))
        dict['Area'] = energy_node.xpath('//PArea/text()'.lower())[0]
        IntervalEnergies=energy_node.xpath('//IntervalEnergies'.lower())
        for interval in IntervalEnergies:
            interval_node = etree.HTML(etree.tostring(interval))
            dict['Interval'] = interval_node.xpath('//Interval/text()'.lower())[0]
            dict['TotalScheduledOR'] = interval_node.xpath('//TotalScheduledOR/text()'.lower())[0]
            dict['TotalCalledOR'] = interval_node.xpath('//TotalCalledOR/text()'.lower())[0]
            dict['RemainingReserve'] = interval_node.xpath('//RemainingReserve/text()'.lower())[0]
            timeStr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1) + '-' + str(
                (int(dict['Interval']) - 1) * 5)
            dict['datetime'] = datetime.datetime.strptime(timeStr, '%Y-%m-%d-%H-%M')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

file_folder='C:/Users/benson/Desktop/2016/Dispatch Area Operating Reserve-Total Scheduled and Total Energy Called/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Dispatch Area Operating Reserve-Total Scheduled and Total Energy Called/'

def generate_list_DispAreaOpResAndEnergyCalled(startHour,endHour,folder):
    hourList = pd.date_range(startHour, endHour, freq='H')
    fileList = []
    # PUB_DispAreaOpResAndEnergyCalled_2016010520_v1
    for hour in hourList:
        month_str = str(hour).split(' ')[0].split('-')
        hourStr = str(hour).split(' ')[1].split(':')[0]
        hourInt = int(hourStr) + 1
        if hourInt < 10:
            fileList.append('%sPUB_DispAreaOpResAndEnergyCalled_%s%s%s0%i_v1.xml' % (
            folder, month_str[0], month_str[1], month_str[2], hourInt))
        else:
            fileList.append(
                '%sPUB_DispAreaOpResAndEnergyCalled_%s%s%s%i_v1.xml' % (folder, month_str[0], month_str[1], month_str[2], hourInt))
    return fileList
def IsSubString(subList,str):
    flag=True
    for subStr in subList:
        if not(subStr in str):
            flag=False
    return flag

def save_csv_DispAreaOpResAndEnergyCalled(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_DispAreaOpResAndEnergyCalled(filename)
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

gen_file_list=generate_list_DispAreaOpResAndEnergyCalled('2016-01-01 00:00','2016-12-31 23:00',file_folder)
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
save_csv_DispAreaOpResAndEnergyCalled(used_list[0])
# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_TRAPreauctionInterfaceHistoryMonthly(),used_list)
# t2=datetime.datetime.now()
# print t2-t1