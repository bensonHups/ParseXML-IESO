from lxml import etree
import pandas as pd
import datetime,time
import multiprocessing
import numpy as np

# file_path='C:/Users/benson/Desktop/2016/PUB_PredispShadowPrices_20160101_v1.xml'
file_path='C:/Users/benson/Downloads/Predispatch Shadow Prices Report.xml'
# PUB_PredispShadowPrices_20160101_v1
def get_DataFrame_PredispShadowPrices(filePath):
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



file_folder='C:/Users/benson/Desktop/2016/Predispatch Shadow Prices Report/'
csv_folder='C:/Users/benson/Desktop/2015-csv/Predispatch Shadow Prices Report/'

def generate_list_filename(startdate,enddate,folder):
    dayList=pd.date_range(startdate,enddate,freq='D')
    dayListStr=[]
    # excep = ['2016021304', '2016081012', '2016081013', '2016081924', '2016082424']
    # PUB_PredispShadowPrices_20160101_v1
    # PUB_PredispShadowPrices_20160101_v32
    for day in dayList:
        daystr=str(day).split(' ')[0].split('-')
        for i in np.arange(1,33,1):
            dayListStr.append('%sPUB_PredispShadowPrices_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
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

def parse_dayandsave(day):
    file_list=generate_list_filename(day,day,file_folder)
    for i in np.arange(0,len(file_list),1):
        df=get_DataFrame_PredispShadowPrices(file_list[i])
        df.to_csv('%s%s-%i.csv' % (csv_folder, day,i), header=True)
        print "saved:%s"%file_list[i]
        time.sleep(2)
    return day

def save_csv(filename):
    t1=datetime.datetime.now()
    df = get_DataFrame_PredispShadowPrices(filename)
    hour=(filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (csv_folder, hour), header=True)
    t2=datetime.datetime.now()
    print 'saved:%s,%s seconds'%(hour,t2-t1)
    time.sleep(2)
    return filename

def print_result(request,result):
    print "result:%s %r"%(request.requestID,result)

def xml_df_parser(xml_folder):
    gen_file_list=generate_list_filename('20160101','20161231',xml_folder)
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
    print '------------------------------------------'

    save_csv(used_list[0])
    t1=datetime.datetime.now()
    print t1
    pool=multiprocessing.Pool(multiprocessing.cpu_count())
    pool.map(save_csv,used_list)
    t2=datetime.datetime.now()
    print t2-t1


day_folder='C:/Users/benson/Desktop/day_data/2016/Predispatch Shadow Prices Report/'

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

def generate_PredispShadowPrices_Table(dayStr,header,hours_forecast):
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
    csv_list=get_csv_list(daystr,csv_folder)
    if len(csv_list)>0:
        df = pd.read_csv(csv_list[0])
        headers=df.groupby(df['NodeName'])
        head_list=[]
        for head in headers:
            head_list.append(head[0])
        headers = df.groupby(df['PriceType'])
        headers_pricetype = []
        for head in headers:
            headers_pricetype.append(head[0])
        headers = []
        for i in range(len(head_list)):
            for j in range(len(headers_pricetype)):
                h_str = 'PredispShadowPrices_%s_%s' % (head_list[i], headers_pricetype[j])
                headers.append(h_str)
        df_save=generate_PredispShadowPrices_Table(daystr,headers,8)
    # --create a save data table--
        print 'len CSV:%i'%len(csv_list)
        for file in csv_list:
            df=pd.read_csv(file)
            for index in df.index:
                str_node = df.loc[index, ['NodeName']][0]
                str_prictType = df.loc[index, ['PriceType']][0]
            # str_schedule_typeID=df.loc[index, ['ScheduleTypeID']][0]
                str_name = 'PredispShadowPrices_%s_%s' % (str_node, str_prictType)
                ctime = df.loc[index, ['CreatedAt']][0]
                ctime_y = datetime.datetime.strptime(ctime, '%Y-%m-%dT%H:%M:%S')
                dtime = df.loc[index, ['datetime']][0]
                dtime_y = datetime.datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
                value=df.loc[index, ['MCP']][0]
                df_save=update_dataframe_value(dtime_y,ctime_y,str_name,value,df_save)
        df_save.to_csv('%sPUB_PredispShadowPrices_%s.csv' % (day_folder,daystr))
    t2=datetime.datetime.now()
    print 'saved:%s'%(t2-t1)

def year_csv2day_PredispShadowPrices():
    t1=datetime.datetime.now()
    print t1
    day_list=pd.date_range('2016-01-01 00:00:00','2016-12-31 00:00:00',freq='D')
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