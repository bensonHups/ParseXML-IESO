from lxml import etree
import pandas as pd
import datetime,time
import numpy as np
import  multiprocessing

file_path='C:/Users/benson/Desktop/2016-example/PUB_VGForecastSummary_20160201_v22.xml'
def get_DataFrame_VGForecastSummary(filePath):
    doc = etree.parse(filePath)
    root= doc.getroot()
    html = etree.HTML(etree.tostring(root))
    data_list=[]
    dict = {}
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat = html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    dict['ForecastTimeStamp']=html.xpath('//DocBody/ForecastTimeStamp/text()'.lower())[0]
    OrganizationData=html.xpath('//DocBody/OrganizationData'.lower())
    for organization in OrganizationData:
        organization_node = etree.HTML(etree.tostring(organization))
        dict['OrganizationType']=organization_node.xpath('//OrganizationType/text()'.lower())[0]
        FuelData=organization_node.xpath('//FuelData'.lower())
        for fuel in FuelData:
            fuel_node = etree.HTML(etree.tostring(fuel))
            dict['FuelType']=fuel_node.xpath('//FuelType/text()'.lower())[0]
            ResourceData=fuel_node.xpath('//ResourceData'.lower())
            for resource in ResourceData:
                resource_node = etree.HTML(etree.tostring(resource))
                dict['ZoneName'] = resource_node.xpath('//ZoneName/text()'.lower())[0]
                EnergyForecast= resource_node.xpath('//EnergyForecast'.lower())
                for energy in EnergyForecast:
                    energy_node = etree.HTML(etree.tostring(energy))
                    dict['ForecastDate']=energy_node.xpath('//ForecastDate/text()'.lower())[0]
                    ForecastInterval= resource_node.xpath('//ForecastInterval'.lower())
                    for interval in ForecastInterval:
                        interval_node = etree.HTML(etree.tostring(interval))
                        dict['ForecastHour'] = interval_node.xpath('//ForecastHour/text()'.lower())[0]
                        dict['MWOutput'] = interval_node.xpath('//MWOutput/text()'.lower())[0]
                        timeStr = dict['ForecastDate'] + '-' + str(int(dict['ForecastHour']) - 1)
                        dict['datetime'] = datetime.datetime.strptime(timeStr, '%Y-%m-%d-%H')
                        dict2 = {}
                        dict2.update(dict)
                        data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

xml_folder='C:/Users/benson/Desktop/2016/Variable Generation Forecast Summary Report/'
csv_folder='C:/Users/benson/Desktop/IESO/2016/Variable Generation Forecast Summary Report/'
# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

#generate all the fileList
def generate_list_VGForecastSummary(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]
    # PUB_VGForecastSummary_20160201_v24
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        for i in np.arange(1,25,1):
            dayListStr.append('%sPUB_VGForecastSummary_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
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

def save_csv_VGForecastSummary(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_VGForecastSummary(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (csv_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)
def xml_df_parser(xml_folder):
    gen_file_list=generate_list_VGForecastSummary('20160101','20161231',xml_folder)
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
    save_csv_VGForecastSummary(used_list[0])
# t1=datetime.datetime.now()
# print t1
# pool=multiprocessing.Pool(multiprocessing.cpu_count())
# pool.map(save_csv_PredispatchConstrained,used_list)
# t2=datetime.datetime.now()
# print t2-t1

day_folder= 'C:/Users/benson/Desktop/day_data/2016/Variable Generation Forecast Summary Report/'

def generate_VGForecastSummary_Table(dayStr,header):
    day_hourlist=pd.date_range('%s 00:00:00'%dayStr,'%s 23:00:00'%dayStr,freq='H')
    dict = {}
    data_list = []
    for i in range(len(day_hourlist)):
        dict['datetime']=day_hourlist[i]
        for j in range(len(header)):
            str=header[j]
            dict['%s_DA1'%str]=None
            dict['%s_DA2'%str] = None
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
    day_dif=date_dif(timestamp,creatat)
    if day_dif==1:
        column_name='%s_DA1'%name
        dataframe.set_value(timestamp, column_name, value, takeable=False)
    elif day_dif==2:
        column_name = '%s_DA2' % name
        dataframe.set_value(timestamp, column_name, value, takeable=False)
    return dataframe

def get_csv_list(daystr,folder):
    f_list=get_list_filename(folder,['.csv'])
    day_list=[]
    for file in f_list:
        if file.find(daystr)>=0:
            day_list.append(file)
    return day_list

def time_index_dataframe(daystr):
    t1=datetime.datetime.now()
    day_time=datetime.datetime.strptime(daystr,'%Y%m%d')
    day1_before=day_time-datetime.timedelta(days=1)
    day1_str=day1_before.strftime('%Y%m%d')
    day2_before = day_time - datetime.timedelta(days=2)
    day2_str = day2_before.strftime('%Y%m%d')
    # print 'day1_str:%s day2_str:%s'%(day1_str,day2_str)
    csv1_list=get_csv_list(day1_str,csv_folder)
    print csv1_list
    csv2_list=get_csv_list(day2_str,csv_folder)
    print csv2_list
    csv_list=[]
    if len(csv1_list)>0:
        csv_list.append(csv1_list[0])
    if len(csv2_list) > 0:
        csv_list.append(csv2_list[0])
    if len(csv_list)>0:
        df = pd.read_csv(csv_list[0])
        headers=df.groupby(df['ZoneName'])
        head_list=[]
        for head in headers:
            head_list.append(head[0])

        headers = df.groupby(df['OrganizationType'])
        headers_organizationtype = []
        for head in headers:
            headers_organizationtype.append(head[0])

        headers = df.groupby(df['FuelType'])
        headers_FuelType = []
        for head in headers:
            headers_FuelType.append(head[0])

        headers = []
        for i in range(len(head_list)):
            for j in range(len(headers_organizationtype)):
                for k in range(len(headers_FuelType)):
                    h_str = 'VGForecastSummary_%s_%s_%s' % (head_list[i], headers_organizationtype[j],headers_FuelType[k])
                    headers.append(h_str)
        df_save=generate_VGForecastSummary_Table(daystr,headers)
    # --create a save data table--

        for file in csv_list:
            df=pd.read_csv(file)
            for index in df.index:
                str_ZoneName = df.loc[index, ['ZoneName']][0]
                str_OrganizationType = df.loc[index, ['OrganizationType']][0]
                str_FuelType = df.loc[index, ['FuelType']][0]
            # str_schedule_typeID=df.loc[index, ['ScheduleTypeID']][0]
                str_name = 'VGForecastSummary_%s_%s_%s' % (str_ZoneName, str_OrganizationType,str_FuelType)
                ctime = df.loc[index, ['CreatedAt']][0]
                ctime_y = datetime.datetime.strptime(ctime, '%Y-%m-%dT%H:%M:%S')
                dtime = df.loc[index, ['datetime']][0]
                ddtime=datetime.datetime.strptime(daystr,'%Y%m%d')
                dtime_y = datetime.datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
                value=df.loc[index, ['MWOutput']][0]
                if ddtime.date()==dtime_y.date():
                    df_save=update_dataframe_value(dtime_y,ctime_y,str_name,value,df_save)
        df_save.to_csv('%sVGForecastSummary_%s.csv' % (day_folder,daystr))
    t2=datetime.datetime.now()
    print 'saved:%s'%(t2-t1)
def year_csv2day_PredispShadowPrices():
    t1=datetime.datetime.now()
    print t1
    day_list=pd.date_range('2016-01-02 00:00:00','2016-12-31 00:00:00',freq='D')
    day_str=[]
    for day in day_list:
        dstr=str(day).split(' ')[0]
        dstr=dstr.replace('-','')
        day_str.append(dstr)
    print day_str
    time_index_dataframe(day_str[0])
    # try:
    #     pool=multiprocessing.Pool(multiprocessing.cpu_count())
    #     pool.map(time_index_dataframe,day_list)
    # except:
    #     print 'here something wrong'
    t2=datetime.datetime.now()
    print t2-t1

year_csv2day_PredispShadowPrices()