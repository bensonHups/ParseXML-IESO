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
csv_folder='/home/peak/IESO-CSV/2016/Monthly Historical Interface Flows-Schedules- and Transmission Transfer Capability Realtime Intertie Scheduling Limits/'

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

def xml_df_parser(xml_folder):
    gen_file_list=generate_list_TRAPreauctionInterfaceHistoryMonthly('2016-01','2017-04',xml_folder)
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
    for i in range(len(used_list)):
        save_csv_TRAPreauctionInterfaceHistoryMonthly(used_list[i])
# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_TRAPreauctionInterfaceHistoryMonthly(),used_list)
# t2=datetime.datetime.now()
# print t2-t1


day_folder= '/home/peak/IESO-DAY/2016/Monthly Historical Interface Flows-Schedules- and Transmission Transfer Capability Realtime Intertie Scheduling Limits/'
def is_datetime_equal(t1,t2):
    t=t1-t2
    if t.seconds!=0:
        return False
    if t.days!=0:
        return False
    return True
def hour_dif(t1,t2):
    t=t1-t2
    hour_t=t.days*24+(t.seconds)/(60*60)
    return hour_t

def get_csv_list(folder):
    f_list=get_list_filename(folder,['.csv'])
    day_list=[]
    for file in f_list:
        if file.find('v29')>=0:
            day_list.append(file)
    return day_list

def generate_TRAPreauctionInterfaceHistoryMonthly_Table(header):
    day_hourlist = pd.date_range('2016-01-01 00:00:00', '2016-12-31 23:00:00', freq='H')
    dict = {}
    data_list = []
    for i in range(len(day_hourlist)):
        dict['datetime'] = day_hourlist[i]
        for j in range(len(header)):
            str = header[j]
            dict[str] = None

        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    df_new = pd.DataFrame.from_dict(data_list)
    df_new = df_new.set_index('datetime')
    return df_new

def update_dataframe_value(timestamp,name,value,dataframe):
    dataframe.set_value(timestamp, name, value, takeable=False)
    return dataframe

def get_list_filename(file_folder, FlagStr=[]):
    import os
    fileList = []
    fileNames = os.listdir(file_folder)
    if len(fileNames) > 0:
        for file in fileNames:
            if len(FlagStr) > 0:
                if IsSubString(FlagStr, file):
                    fileList.append(os.path.join(file_folder, file))
            else:
                fileList.append(os.path.join(file_folder, file))
    if len(fileList) > 0:
        fileList.sort()
    return fileList

def time_index_dataframe(day_folder):
    t1=datetime.datetime.now()
    print t1
    csv_list=get_list_filename(csv_folder,['.csv'])
    print csv_list
    df = pd.read_csv(csv_list[0])
    headers=df.groupby(df['IntertieZoneName'])
    headers_node = []
    for head in headers:
        headers_node.append(head[0])
    headers_pricetype=['ScheduledFlow','ActualFlow','IntertieTransferCapabilityIn','IntertieTransferCapabilityOut']
    headers=[]
    for i in range(len(headers_node)):
        for j in range(len(headers_pricetype)):
            h_str='TRAPInterfaceMonth_%s_%s'%(headers_node[i],headers_pricetype[j])
            headers.append(h_str)
    df_save=generate_TRAPreauctionInterfaceHistoryMonthly_Table(headers)
    # # --create a save data table--
    for file in csv_list:
        df=pd.read_csv(file)
        for index in df.index:
            str_nodename = df.loc[index, ['IntertieZoneName']][0]
            dtime = df.loc[index, ['datetime']][0]
            dtime_y = datetime.datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')

            ScheduledFlow= df.loc[index, ['ScheduledFlow']][0]
            str_name='TRAPInterfaceMonth_%s_ScheduledFlow'%str_nodename
            df_save = update_dataframe_value(dtime_y, str_name, ScheduledFlow, df_save)

            ActualFlow = df.loc[index, ['ActualFlow']][0]
            str_name = 'TRAPInterfaceMonth_%s_ActualFlow' % str_nodename
            df_save = update_dataframe_value(dtime_y, str_name, ActualFlow, df_save)

            IntertieTransferCapabilityIn = df.loc[index, ['IntertieTransferCapabilityIn']][0]
            str_name = 'TRAPInterfaceMonth_%s_IntertieTransferCapabilityIn' % str_nodename
            df_save = update_dataframe_value(dtime_y, str_name, IntertieTransferCapabilityIn, df_save)

            IntertieTransferCapabilityOut = df.loc[index, ['IntertieTransferCapabilityOut']][0]
            str_name = 'TRAPInterfaceMonth_%s_IntertieTransferCapabilityOut' % str_nodename
            df_save = update_dataframe_value(dtime_y, str_name, IntertieTransferCapabilityOut, df_save)

        t2=datetime.datetime.now()
        print t2
    df_save.to_csv('%sPUB_TRAPInterfaceMonth_2016.csv' % (day_folder))
    t2=datetime.datetime.now()
    print 'saved:%s'%(t2-t1)

def csv_hour_data():
    t1=datetime.datetime.now()
    print t1
    day_list=pd.date_range('2016-01-01 00:00:00','2016-12-31 23:00:00',freq='D')
    day_str=[]
    for day in day_list:
        dstr=str(day).split(' ')[0]
        dstr=dstr.replace('-','')
        day_str.append(dstr)
    print day_str
    pool=multiprocessing.Pool(multiprocessing.cpu_count())
    pool.map(time_index_dataframe,day_str)
    t2=datetime.datetime.now()
    print t2-t1

time_index_dataframe(day_folder)


