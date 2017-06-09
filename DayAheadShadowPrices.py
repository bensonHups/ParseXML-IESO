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
csv_folder='C:/Users/benson/Desktop/2015-csv/Day-Ahead Shadow Prices Report'

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
    gen_file_list=generate_list_DAShadowPrices('20160101','20161231',xml_folder)
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

day_folder='C:/Users/benson/Desktop/day_data/2016/Day-Ahead Shadow Prices Report/'


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

def get_csv_list(daystr,folder):
    f_list=get_list_filename(folder,['.csv'])
    day_list=[]
    for file in f_list:
        if file.find(daystr)>=0:
            day_list.append(file)
    return day_list

def generate_DAShadowPrices_Table(dayStr,header):
    day_hourlist=pd.date_range('%s 00:00:00'%dayStr,'%s 23:00:00'%dayStr,freq='H')
    dict = {}
    data_list = []
    for i in range(len(day_hourlist)):
        dict['datetime']=day_hourlist[i]
        for j in range(len(header)):
            str=header[j]
            dict['%s_DAShadowPrices_DAM'%str]=None
            dict['%s_DAShadowPrices_DAF'%str]=None
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)

    df_new = pd.DataFrame.from_dict(data_list)
    df_new=df_new.set_index('datetime')
    return df_new

def update_dataframe_value(timestamp,creatat,name,value,dataframe):
    h=creatat.hour
    if h<12:
        column_name='%s_DAShadowPrices_DAM'%name
        dataframe.set_value(timestamp, column_name, value, takeable=False)
    elif h<16:
        column_name='%s_DAShadowPrices_DAF'%name
        dataframe.set_value(timestamp, column_name, value, takeable=False)
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

def time_index_dataframe(daystr):
    t1=datetime.datetime.now()
    print t1
    csv_list=get_csv_list(daystr,csv_folder)
    print csv_list
    df = pd.read_csv(csv_list[0])
    headers=df.groupby(df['NodeName'])
    headers_node = []
    for head in headers:
        headers_node.append(head[0])
    headers = df.groupby(df['PriceType'])
    headers_pricetype = []
    for head in headers:
        headers_pricetype.append(head[0])
    headers=[]
    for i in range(len(headers_node)):
        for j in range(len(headers_pricetype)):
            h_str='%s_%s'%(headers_node[i],headers_pricetype[j])
            headers.append(h_str)
    df_save=generate_DAShadowPrices_Table(daystr,headers)
    # # --create a save data table--
    for file in csv_list:
        df=pd.read_csv(file)
        for index in df.index:
            str_node = df.loc[index, ['NodeName']][0]
            str_prictType= df.loc[index, ['PriceType']][0]
            # str_schedule_typeID=df.loc[index, ['ScheduleTypeID']][0]
            str_name='%s_%s'%(str_node,str_prictType)
            ctime = df.loc[index, ['CreatedAt']][0]
            ctime_y = datetime.datetime.strptime(ctime, '%Y-%m-%dT%H:%M:%S')
            dtime = df.loc[index, ['datetime']][0]
            dtime_y = datetime.datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
            value=df.loc[index, ['MCP']][0]
            df_save=update_dataframe_value(dtime_y,ctime_y,str_name,value,df_save)
        t2=datetime.datetime.now()
        print t2
    df_save.to_csv('%sPUB_DAShadowPrices_%s.csv' % (day_folder,daystr))
    # print '%sPUB_DAIntertieSchedLimits_%s.csv' % (day_folder,daystr)
    t2=datetime.datetime.now()
    print 'saved:%s'%(t2-t1)

def csv_hour_data():
    t1=datetime.datetime.now()
    print t1
    day_list=pd.date_range('2016-01-01 00:00:00','2016-10-03 23:00:00',freq='D')
    day_str=[]
    for day in day_list:
        dstr=str(day).split(' ')[0]
        dstr=dstr.replace('-','')
        day_str.append(dstr)
    print day_str
    time_index_dataframe(day_str[0])
    # try:
    #     pool=multiprocessing.Pool(multiprocessing.cpu_count())
    #     pool.map(time_index_dataframe,day_str)
    # except:
    #     print 'sth wrong!'
    t2=datetime.datetime.now()
    print t2-t1
csv_hour_data()


