from lxml import etree
import pandas as pd
import datetime,time
import numpy as np
import multiprocessing
# file_path='C:/Users/benson/Desktop/2016/PUB_OntarioZonalDemand_20160621_v1.xml'
file_path='C:/Users/benson/Downloads/Ontario Zonal Demand Forecast Report.xml'
def get_DataFrame_OntarioZonalDemand(filePath):
    doc = etree.parse(filePath)
    root = doc.getroot()
    html = etree.HTML(etree.tostring(root))
    data_list = []
    dict = {}
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat = html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    dict['DemandForecastStartDate']=html.xpath('//DemandForecastStartDate/text()'.lower())[0]
    dict['DemandForecastEndDate'] = html.xpath('//DemandForecastEndDate/text()'.lower())[0]
    ZonalDemands = html.xpath('//DocBody/ZonalDemands'.lower())
    for zone in ZonalDemands:
        zone_node=etree.HTML(etree.tostring(zone))
        dict['DeliveryDate']=zone_node.xpath('//DeliveryDate/text()'.lower())[0]
        Ontario_Demands=zone_node.xpath('//Ontario/Demand'.lower())
        for odemand in Ontario_Demands:
            odemand_node = etree.HTML(etree.tostring(odemand))
            dict['zone']='Ontario'
            dict['DeliveryHour'] = odemand_node.xpath('//DeliveryHour/text()'.lower())[0]
            dict['EnergyMW'] = odemand_node.xpath('//EnergyMW/text()'.lower())[0]
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        East_Demands = zone_node.xpath('//East/Demand'.lower())
        for edemand in East_Demands:
            edemand_node = etree.HTML(etree.tostring(edemand))
            dict['zone']='East'
            dict['DeliveryHour'] = edemand_node.xpath('//DeliveryHour/text()'.lower())[0]
            dict['EnergyMW'] = edemand_node.xpath('//EnergyMW/text()'.lower())[0]
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        West_Demands = zone_node.xpath('//West/Demand'.lower())
        for wdemand in West_Demands:
            wdemand_node = etree.HTML(etree.tostring(wdemand))
            dict['zone'] = 'West'
            dict['DeliveryHour'] = wdemand_node.xpath('//DeliveryHour/text()'.lower())[0]
            dict['EnergyMW'] = wdemand_node.xpath('//EnergyMW/text()'.lower())[0]
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)


    # for i in range(len(ZonalDemands)):
    #     DeliveryDate=html.xpath(((path0+'/DeliveryDate')%(i+1)).lower())
    #     dict['DeliveryDate']=DeliveryDate[0].text
    #     dict['zone']='Ontario'
    #     Demand=html.xpath(((path0 + '/ZonalDemand/Ontario/Demand') % (i + 1)).lower())
    #     path1_1=path0 + '/ZonalDemand/Ontario/Demand[%i]'
    #     for j in range(len(Demand)):
    #         DeliveryHour=html.xpath(((path1_1+'/DeliveryHour')%(i+1,j+1)).lower())
    #         dict['DeliveryHour'] = DeliveryHour[0].text
    #         EnergyMW=html.xpath(((path1_1+'/EnergyMW')%(i+1,j+1)).lower())
    #         dict['EnergyMW'] = EnergyMW[0].text
    #         timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
    #         dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
    #         dict2 = {}
    #         dict2.update(dict)
    #         data_list.append(dict2)
    # for i in range(len(ZonalDemands)):
    #     DeliveryDate = html.xpath(((path0 + '/DeliveryDate') % (i + 1)).lower())
    #     dict['DeliveryDate'] = DeliveryDate[0].text
    #     dict['zone'] = 'East'
    #     Demand = html.xpath(((path0 + '/ZonalDemand/East/Demand') % (i + 1)).lower())
    #     path1_1 = path0 + '/ZonalDemand/East/Demand[%i]'
    #     for j in range(len(Demand)):
    #         DeliveryHour = html.xpath(((path1_1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
    #         dict['DeliveryHour'] = DeliveryHour[0].text
    #         EnergyMW = html.xpath(((path1_1 + '/EnergyMW') % (i + 1, j + 1)).lower())
    #         dict['EnergyMW'] = EnergyMW[0].text
    #         timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
    #         dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
    #         dict2 = {}
    #         dict2.update(dict)
    #         data_list.append(dict2)
    # for i in range(len(ZonalDemands)):
    #     DeliveryDate = html.xpath(((path0 + '/DeliveryDate') % (i + 1)).lower())
    #     dict['DeliveryDate'] = DeliveryDate[0].text
    #     dict['zone'] = 'West'
    #     Demand = html.xpath(((path0 + '/ZonalDemand/West/Demand') % (i + 1)).lower())
    #     path1_1 = path0 + '/ZonalDemand/West/Demand[%i]'
    #     for j in range(len(Demand)):
    #         DeliveryHour = html.xpath(((path1_1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
    #         dict['DeliveryHour'] = DeliveryHour[0].text
    #         EnergyMW = html.xpath(((path1_1 + '/EnergyMW') % (i + 1, j + 1)).lower())
    #         dict['EnergyMW'] = EnergyMW[0].text
    #         timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
    #         dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
    #         dict2 = {}
    #         dict2.update(dict)
    #         data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)


file_folder='C:/Users/benson/Desktop/2016/Ontario Zonal Demand Forecast/'
csv_folder='C:/Users/benson/Desktop/2015-csv/Ontario Zonal Demand Forecast/'
# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

#generate all the fileList
def generate_list_OntarioZonalDemand(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]

    # PUB_OntarioZonalDemand_20160621_v1
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        dayListStr.append('%sPUB_OntarioZonalDemand_%s%s%s_v1.xml' % (folder, daystr[0], daystr[1], daystr[2]))
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

def save_csv_OntarioZonalDemand(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_OntarioZonalDemand(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (csv_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)

def xml_day_parser(xml_folder):
    gen_file_list=generate_list_OntarioZonalDemand('20160621','20161231',xml_folder)
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
    print '--------------------------len:%i----------------'%len(used_list)
    save_csv_OntarioZonalDemand(used_list[0])

# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_PredispAreaOpResShortfalls,used_list)
# t2=datetime.datetime.now()
# print t2-t1

day_folder= 'C:/Users/benson/Desktop/day_data/2016/Ontario Zonal Demand Forecast/'

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

def generate_OntarioZonalDemand_Table(daystr,header,days_forecast):
    day_hourlist=pd.date_range('%s 00:00:00'%daystr,'%s 23:55:00'%daystr,freq='H')
    dict = {}
    data_list = []
    for i in range(len(day_hourlist)):
        dict['datetime']=day_hourlist[i]
        for j in range(len(header)):
            str=header[j]
            dict[str]=None
            for k in range(1,days_forecast+1,1):
                dict['%s_DA%i'%(str,k)]=None
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)

    df_new = pd.DataFrame.from_dict(data_list)
    df_new=df_new.set_index('datetime')
    return df_new
def date_dif(t1,t2):
    t=t1.date()-t2.date()
    return t.days
def update_dataframe_value(timestamp,creatat,name,value,dataframe):
    h=date_dif(timestamp,creatat)
    if h==0:
        column_name=name
        print '%s:%s-%s'%(timestamp,name,value)
        dataframe.set_value(timestamp, column_name, value, takeable=False)
    elif h<7 and h>0:
        column_name='%s_DA%i'%(name,h)
        print '%s:%s-%s' % (timestamp, column_name, value)
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

def get_weekbefore_list(daystr,xml_folder):
    t1=datetime.datetime.strptime(daystr,'%Y%m%d')
    t2=t1-datetime.timedelta(hours=6*24)
    time_list=pd.date_range(t2,t1,freq='D')
    day_str=[]
    for tday in time_list:
        print tday
        t_date=datetime.datetime.strftime(tday,'%Y%m%d')
        day_str.append(t_date)
    file_list=[]
    for dstr in day_str:
        f_list=get_csv_list(dstr,xml_folder)
        for f in f_list:
            file_list.append(f)
    return file_list

def time_index_dataframe(daystr):
    t1=datetime.datetime.now()
    print t1
    csv_list=get_weekbefore_list(daystr,csv_folder)
    day_time=datetime.datetime.strptime(daystr,'%Y%m%d')
    print csv_list
    df = pd.read_csv(csv_list[0])
    headers = df.groupby(df['zone'])
    headers_node = []
    for head in headers:
        headers_node.append(head[0])
    headers=[]
    for head_zone in headers_node:
        headers.append('OntarioZonalDemandForecast_%s'%head_zone)
    df_save=generate_OntarioZonalDemand_Table(daystr,headers,6)
    # # --create a save data table--
    for file in csv_list:
        df=pd.read_csv(file)
        for index in df.index:
            str_zone = df.loc[index, ['zone']][0]
            # str_prictType= df.loc[index, ['PriceType']][0]
            # str_schedule_typeID=df.loc[index, ['ScheduleTypeID']][0]
            str_name='OntarioZonalDemandForecast_%s'%str_zone
            ctime = df.loc[index, ['CreatedAt']][0]
            ctime_y = datetime.datetime.strptime(ctime, '%Y-%m-%dT%H:%M:%S')
            dtime = df.loc[index, ['datetime']][0]
            dtime_y = datetime.datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
            value=df.loc[index, ['EnergyMW']][0]
            if date_dif(dtime_y,day_time)==0:
                df_save=update_dataframe_value(dtime_y,ctime_y,str_name,value,df_save)
        t2=datetime.datetime.now()
        print t2
    df_save.to_csv('%sPUB_OntarioZonalDemandForecast_%s.csv' % (day_folder,daystr))
    t2=datetime.datetime.now()
    print 'saved:%s'%(t2-t1)

def csv_hour_data():
    t1=datetime.datetime.now()
    print t1
    day_list=pd.date_range('2016-06-27 00:00:00','2016-12-31 23:00:00',freq='D')
    day_str=[]
    for day in day_list:
        dstr=str(day).split(' ')[0]
        dstr=dstr.replace('-','')
        day_str.append(dstr)
    time_index_dataframe(day_str[0])
    # print day_str
    # pool=multiprocessing.Pool(multiprocessing.cpu_count())
    # pool.map(time_index_dataframe,day_str)
    # t2=datetime.datetime.now()
    # print t2-t1

csv_hour_data()