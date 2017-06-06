from lxml import etree
import pandas as pd
import datetime,time
import  multiprocessing
# file_path='C:/Users/benson/Desktop/2016/PUB_RealtimeMktTotals_2016010101_v1.xml'

file_path='C:/Users/benson/Downloads/Realtime Market Totals Report.xml'
def get_DataFrame_RealtimeMarketTotals(filePath):
    doc = etree.parse(filePath)
    root= doc.getroot()
    html = etree.HTML(etree.tostring(root))
    data_list=[]
    dict = {}
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat = html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    deliverydate = html.xpath('//DocBody/DeliveryDate'.lower())
    dict['DeliveryDate'] = deliverydate[0].text
    DeliveryHour = html.xpath('//DocBody/DeliveryHour'.lower())
    dict['DeliveryHour'] = DeliveryHour[0].text
    IntervalEnergy=html.xpath('//Energies/IntervalEnergy'.lower())
    for energy in IntervalEnergy:
        node = etree.HTML(etree.tostring(energy))
        dict['Interval']=node.xpath('//Interval/text()'.lower())[0]
        MQ=node.xpath('//MQ'.lower())
        for mq in MQ:
            mq_node=etree.HTML(etree.tostring(mq))
            dict['MarketQuantity']=mq_node.xpath('//MarketQuantity/text()'.lower())[0]
            dict['EnergyMW'] = mq_node.xpath('//EnergyMW/text()'.lower())[0]
    # for i in range(len(IntervalEnergy)):
    #     Interval=html.xpath(('//Energies/IntervalEnergy[%i]/Interval'%(i+1)).lower())
    #     dict['Interval'] = Interval[0].text
    #     MQ=html.xpath(('//Energies/IntervalEnergy[%i]/MQ/MarketQuantity'%(i+1)).lower())
    #     for j in range(len(MQ)):
    #         MarketQuantity=html.xpath(('//Energies/IntervalEnergy[%i]/MQ[%i]/MarketQuantity'%(i+1,j+1)).lower())
    #         dict['MarketQuantity']=MarketQuantity[0].text
    #         EnergyMW=html.xpath(('//Energies/IntervalEnergy[%i]/MQ[%i]/EnergyMW'%(i+1,j+1)).lower())
    #         dict['EnergyMW'] = EnergyMW[0].text
            timeStr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1) + '-' + str(
                (int(dict['Interval']) - 1) * 5)
            dict['datetime'] = datetime.datetime.strptime(timeStr, '%Y-%m-%d-%H-%M')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

file_folder='C:/Users/benson/Desktop/2016/Realtime Market totals Report/'
csv_folder='C:/Users/benson/Desktop/IESO/2016/Realtime Market totals Report/'

def generate_list_RealtimeMarketTotals(startHour,endHour,folder):
    hourList=pd.date_range(startHour,endHour,freq='H')
    fileList=[]
    # PUB_RealtimeMktTotals_2016010101_v1
    for hour in hourList:
        month_str=str(hour).split(' ')[0].split('-')
        hourStr=str(hour).split(' ')[1].split(':')[0]
        hourInt=int(hourStr)+1
        if hourInt<10:
            fileList.append('%sPUB_RealtimeMktTotals_%s%s%s0%i_v1.xml' % (folder, month_str[0], month_str[1], month_str[2], hourInt))
        else:
            fileList.append(
                '%sPUB_RealtimeMktTotals_%s%s%s%i_v1.xml' % (folder, month_str[0], month_str[1], month_str[2], hourInt))
    return fileList
def IsSubString(subList,str):
    flag=True
    for subStr in subList:
        if not(subStr in str):
            flag=False
    return flag
def save_csv_RealtimeMarketTotals(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_RealtimeMarketTotals(filename)
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
def year_xml2df_RealtimeMarketTotals(xml_folder):
    gen_file_list=generate_list_RealtimeMarketTotals('2016-01-01 00:00','2016-12-31 23:00',xml_folder)
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
        if flag:
            print gen_str
    print '-----------------%i-------------------------'%len(used_list)
# for i in range(len(used_list)):
#     save_csv_RealtimeMarketTotals(used_list[0])
    t1=datetime.datetime.now()
    print t1
    pool=multiprocessing.Pool(multiprocessing.cpu_count())
    pool.map(save_csv_RealtimeMarketTotals,used_list)
    t2=datetime.datetime.now()
    print t2-t1


day_folder= 'C:/Users/benson/Desktop/2015-day/Realtime Market totals Report/'

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

def generate_RealtimeMarketTotals_Table(dayStr,header):
    day_hourlist=pd.date_range('%s 00:00:00'%dayStr,'%s 23:55:00'%dayStr,freq='5min')
    dict = {}
    data_list = []
    for i in range(len(day_hourlist)):
        dict['datetime']=day_hourlist[i]
        for j in range(len(header)):
            str=header[j]
            dict[str]=None
            # for k in range(1,hours_forecast+1,1):
            #     dict['%s_F%i'%(str,k)]=None
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)

    df_new = pd.DataFrame.from_dict(data_list)
    df_new=df_new.set_index('datetime')
    return df_new

def update_dataframe_value(timestamp,creatat,name,value,dataframe):
    h=hour_dif(timestamp,creatat)
    if h<=0:
        column_name=name
        dataframe.set_value(timestamp, column_name, value, takeable=False)
    elif h<9:
        column_name='%s_F%i'%(name,h)
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
    headers = df.groupby(df['MarketQuantity'])
    headers_node = []
    for head in headers:
        headers_node.append(head[0])
    df_save=generate_RealtimeMarketTotals_Table(daystr,headers_node)
    # # --create a save data table--
    for file in csv_list:
        df=pd.read_csv(file)
        for index in df.index:
            str_name = df.loc[index, ['MarketQuantity']][0]
            # str_prictType= df.loc[index, ['PriceType']][0]
            # str_schedule_typeID=df.loc[index, ['ScheduleTypeID']][0]
            # str_name='%s_%s_%s'%(str_nodename,str_prictType,str_schedule_typeID)
            ctime = df.loc[index, ['CreatedAt']][0]
            ctime_y = datetime.datetime.strptime(ctime, '%Y-%m-%dT%H:%M:%S')
            dtime = df.loc[index, ['datetime']][0]
            dtime_y = datetime.datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
            value=df.loc[index, ['EnergyMW']][0]
            df_save=update_dataframe_value(dtime_y,ctime_y,str_name,value,df_save)
        t2=datetime.datetime.now()
        print t2
    df_save.to_csv('%sPUB_RealtimeMarketTotals_%s.csv' % (day_folder,daystr))
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

csv_hour_data()

