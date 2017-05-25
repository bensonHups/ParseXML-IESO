from lxml import etree
import pandas as pd
import datetime,time
import multiprocessing
file_path='C:/Users/benson/Desktop/2016/PUB_RealtimeIntertieSchedLimits_2016010101_v1.xml'
# file_path='C:/Users/benson/Downloads/Realtime Intertie Scheduling Limits Report.xml'
def get_DataFrame_RealtimeIntertieSchedLimits(filePath):
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
    DeliveryHour = html.xpath('//DocBody/DeliveryHour'.lower())
    dict['DeliveryHour'] = DeliveryHour[0].text
    IntertieZonalEnergies= html.xpath('//DocBody/IntertieZonalEnergies'.lower())

    for zonal_energy in IntertieZonalEnergies:
        zonal_energy_node=etree.HTML(etree.tostring(zonal_energy))
        dict['Interval'] = zonal_energy_node.xpath('//IntertieZoneName/text()'.lower())[0]
        IntervalEnergies=zonal_energy_node.xpath('//IntervalEnergies'.lower())
        for interval_energy in IntervalEnergies:
            interval_energy_node = etree.HTML(etree.tostring(interval_energy))
            dict['PriceType'] = interval_energy_node.xpath('//PriceType/text()'.lower())[0]
            IntervalEnergy=zonal_energy_node.xpath('//IntervalEnergy'.lower())
            for interval in IntervalEnergy:
                interval_node=etree.HTML(etree.tostring(interval))
                dict['Interval']= interval_node.xpath('//Interval/text()'.lower())[0]
                dict['EnergyMW']= interval_node.xpath('//EnergyMW/text()'.lower())[0]
                dict['Flag']= interval_node.xpath('//Flag/text()'.lower())[0]
                # for i in range(len(IntertieZonalEnergies)):
    #     path1_1=((path0+'/IntertieZoneName')%(i+1)).lower()
    #     IntertieZoneName=html.xpath(path1_1)
    #     dict['IntertieZoneName']=IntertieZoneName[0].text
    #     # print IntertieZoneName[0].text
    #     path1=((path0+'/IntervalEnergies[%i]')).lower()
    #     IntervalEnergies=html.xpath(((path0+'/IntervalEnergies')%(i+1)).lower())
    #     for j in range(len(IntervalEnergies)):
    #         PriceType=html.xpath(((path1+'/PriceType')%(i+1,j+1)).lower())
    #         dict['PriceType']=PriceType[0].text
    #         IntervalEnergy=html.xpath(((path1+'/IntervalEnergy')%(i+1,j+1)).lower())
    #         path2=(path1+'/IntervalEnergy[%i]')
    #         for k in range(len(IntervalEnergy)):
    #             Interval = html.xpath(((path2 + '/Interval') % (i + 1, j + 1,k+1)).lower())
    #             dict['Interval'] = Interval[0].text
    #             EnergyMW = html.xpath(((path2 + '/EnergyMW') % (i + 1, j + 1, k + 1)).lower())
    #             dict['EnergyMW'] = EnergyMW[0].text
    #             Flag = html.xpath(((path2 + '/Flag') % (i + 1, j + 1, k + 1)).lower())
    #             dict['Flag'] = Flag[0].text
                timeStr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1) + '-' + str(
                    (int(dict['Interval']) - 1) * 5)
                dict['datetime'] = datetime.datetime.strptime(timeStr, '%Y-%m-%d-%H-%M')

                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

file_folder='C:/Users/benson/Desktop/2016/Realtime Intertie Scheduling Limits/'
save_folder='C:/Users/benson/Desktop/IESO/2016/Realtime Intertie Scheduling Limits/'

def generate_list_RealtimeIntertieSchedLimits(startHour,endHour,folder):
    hourList=pd.date_range(startHour,endHour,freq='H')
    fileList=[]
    # PUB_RealtimeIntertieSchedLimits_2016010101_v1
    for hour in hourList:
        month_str=str(hour).split(' ')[0].split('-')
        hourStr=str(hour).split(' ')[1].split(':')[0]
        hourInt=int(hourStr)+1
        if hourInt<10:
            fileList.append('%sPUB_RealtimeIntertieSchedLimits_%s%s%s0%i_v1.xml' % (folder, month_str[0], month_str[1], month_str[2], hourInt))
        else:
            fileList.append(
                '%sPUB_RealtimeIntertieSchedLimits_%s%s%s%i_v1.xml' % (folder, month_str[0], month_str[1], month_str[2], hourInt))
    return fileList
def IsSubString(subList,str):
    flag=True
    for subStr in subList:
        if not(subStr in str):
            flag=False
    return flag
def save_csv_RealtimeMarketTotals(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_RealtimeIntertieSchedLimits(filename)
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

gen_file_list=generate_list_RealtimeIntertieSchedLimits('2016-01-01 00:00','2016-12-31 23:00',file_folder)
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
# for i in range(len(used_list)):
#     save_csv_RealtimeORIM(used_list[i])
save_csv_RealtimeMarketTotals(used_list[0])
# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_RealtimeMarketTotals,used_list)
# t2=datetime.datetime.now()
# print t2-t1




