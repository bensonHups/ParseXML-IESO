from lxml import etree
import pandas as pd
import datetime,time
import numpy as np
import multiprocessing
file_path='C:/Users/benson/Desktop/2016-example/PUB_PredispConstTotals_20160101_v1.xml'
def get_DataFrame_PredispatchConstrained(filePath):
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
    dict['deliverydate'] = deliverydate[0].text
    HourlyConstrainedEnergy=html.xpath('//HourlyConstrainedEnergy'.lower())
    for hour in HourlyConstrainedEnergy:
        hour_node=etree.HTML(etree.tostring(hour))
        dict['DeliveryHour']=hour_node.xpath('//DeliveryHour/text()'.lower())[0]
        MQ=hour_node.xpath('//MQ'.lower())
        for mq in MQ:
            mq_node=etree.HTML(etree.tostring(mq))
            dict['MarketQuantity'] = mq_node.xpath('//MarketQuantity/text()'.lower())[0]
            dict['EnergyMW'] = mq_node.xpath('//EnergyMW/text()'.lower())[0]
            timestr = dict['deliverydate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

year=2015
xml_folder='/home/peak/Dropbox (Peak Power Inc)/IESO/IESO_Organized/%s/Predispatch Constrained Totals Report/'%year
csv_folder='/home/peak/IESO-CSV/%s/Predispatch Constrained Totals Report/'%year
day_folder='/home/peak/IESO-DAY/%s/Predispatch Constrained Totals Report/'%year
# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

#generate all the fileList
def generate_list_PredispatchConstrained(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]
    # PUB_PredispConstTotals_20160101_v32
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        for i in np.arange(1,33,1):
            dayListStr.append('%sPUB_PredispConstTotals_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
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

def save_csv_PredispatchConstrained(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_PredispatchConstrained(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (csv_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)

def xml_df_parser(xml_folder):
    gen_file_list=generate_list_PredispatchConstrained('%s0101'%year,'%s1231'%year,xml_folder)
    get_list_file=get_list_filename(xml_folder,['.xml'])
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
# for i in range(len(used_list)):
#     save_csv_DayAheadConstrained(used_list[i])
#     save_csv_PredispatchConstrained(used_list[0])
    t1=datetime.datetime.now()
    print t1
    pool=multiprocessing.Pool(multiprocessing.cpu_count())
    pool.map(save_csv_PredispatchConstrained,used_list)
    t2=datetime.datetime.now()
    print t2-t1



def get_csv_list(daystr,folder):
    f_list=get_list_filename(folder,['.csv'])
    day_list=[]
    for file in f_list:
        if file.find(daystr)>=0:
            day_list.append(file)
    return day_list

def hour_dif(t1,t2):
    t=t1-t2
    hour_t=t.days*24+(t.seconds)/(60*60)
    return hour_t

def generate_PredispatchConstrained_Table(dayStr,header,hours_forecast):
    day_hourlist=pd.date_range('%s 00:00:00'%dayStr,'%s 23:00:00'%dayStr,freq='H')
    dict = {}
    data_list = []
    for i in range(len(day_hourlist)):
        dict['datetime']=day_hourlist[i]
        for j in range(len(header)):
            str=header[j]
            dict[str]=None
            for k in range(1,hours_forecast+1,1):
                dict['%s_F%i'%(str,k)]=None
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
    df = pd.read_csv(csv_list[0])
    headers=df.groupby(df['MarketQuantity'])
    head_list=[]
    for head in headers:
       head_list.append(head[0])
    headers=[]
    for i in range(len(head_list)):
        h_str = 'PredispatchConstrained_%s' % head_list[i]
        headers.append(h_str)
    df_save=generate_PredispatchConstrained_Table(daystr,headers,8)
    # --create a save data table--
    print 'len CSV:%i'%len(csv_list)
    for file in csv_list:
        df=pd.read_csv(file)
        for index in df.index:
            str_node = df.loc[index, ['MarketQuantity']][0]
            # str_prictType = df.loc[index, ['PriceType']][0]
            # str_schedule_typeID=df.loc[index, ['ScheduleTypeID']][0]
            str_name = 'PredispatchConstrained_%s' % str_node
            ctime = df.loc[index, ['CreatedAt']][0]
            ctime_y = datetime.datetime.strptime(ctime, '%Y-%m-%dT%H:%M:%S')
            dtime = df.loc[index, ['datetime']][0]
            dtime_y = datetime.datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
            value=df.loc[index, ['EnergyMW']][0]
            df_save=update_dataframe_value(dtime_y,ctime_y,str_name,value,df_save)
    df_save.to_csv('%sPUB_PredispatchConstrained_%s.csv' % (day_folder,daystr))
    t2=datetime.datetime.now()
    print 'saved:%s'%(t2-t1)

def year_csv2day_PredispatchConstrained():
    t1=datetime.datetime.now()
    print t1
    day_list=pd.date_range('%s-01-01 00:00:00'%year,'%s-12-31 00:00:00'%year,freq='D')
    day_str=[]
    for day in day_list:
        dstr=str(day).split(' ')[0]
        dstr=dstr.replace('-','')
        day_str.append(dstr)
    print day_str

    try:
        pool=multiprocessing.Pool(multiprocessing.cpu_count())
        pool.map(time_index_dataframe,day_str)
    except:
        print 'here something wrong'

    t2=datetime.datetime.now()
    print t2-t1

xml_df_parser(xml_folder)
year_csv2day_PredispatchConstrained()


