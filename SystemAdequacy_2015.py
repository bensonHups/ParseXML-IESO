from lxml import etree
import pandas as pd
import datetime, time
import multiprocessing
import numpy as np

file_test='C:/Users/benson/Desktop/2015-example/PUB_Adequacy_20150101_v1.xml'
def get_node_value(node):
    if len(node) > 0:
        return node[0].text
    else:
        return -1.1111

def get_DataFrame_SystemAdequacy_v1(filePath):
    doc = etree.parse(filePath)
    root = doc.getroot()
    html_Str = etree.tostring(root)
    html_Str = html_Str.replace('<Area>', '<PArea>')
    html_Str = html_Str.replace('</Area>', '</PArea>')
    html = etree.HTML(html_Str)
    data_list = []
    dict = {}
    doctitle = html.xpath('//docheader/doctitle')
    dict['DocTitle'] = doctitle[0].text
    createdat = html.xpath('//docheader/createdat')
    dict['CreatedAt'] = createdat[0].text
    deliverydate = html.xpath('//DocBody/DeliveryDate'.lower())
    dict['DeliveryDate'] = deliverydate[0].text
    System = html.xpath('//DocBody/System'.lower())
    for s in System:
        s_node=etree.HTML(etree.tostring(s))
        SystemName_value=s_node.xpath('//SystemName/text()'.lower())[0]
        PeakDemand=s_node.xpath('//PeakDemand/Demand'.lower())
        for demand in PeakDemand:
            demand_node=etree.HTML(etree.tostring(demand))
            dict['DeliveryHour']=demand_node.xpath('//DeliveryHour/text()'.lower())[0]
            EnergyMW = demand_node.xpath('//EnergyMW'.lower())
            dict['EnergyMW'] = get_node_value(EnergyMW)
            dict['SystemName']='%s_PeakDemand'%SystemName_value
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        AverageDemand = s_node.xpath('//AverageDemand/Demand'.lower())
        for demand in AverageDemand:
            demand_node = etree.HTML(etree.tostring(demand))
            dict['DeliveryHour'] = demand_node.xpath('//DeliveryHour/text()'.lower())[0]
            EnergyMW = demand_node.xpath('//EnergyMW'.lower())
            dict['EnergyMW'] = get_node_value(EnergyMW)
            dict['SystemName'] = 'Demand_%s_AverageDemand' % SystemName_value
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        EmbeddedForecasts=s_node.xpath('//EmbeddedForecasts/EmbeddedForecast'.lower())
        for embedded in EmbeddedForecasts:
            embedded_node = etree.HTML(etree.tostring(embedded))
            FuelType_value=embedded_node.xpath('//FuelType/text()'.lower())[0]
            Forecast=embedded_node.xpath('//Forecasts/Forecast')
            for cast in Forecast:
                cast_node = etree.HTML(etree.tostring(cast))
                dict['DeliveryHour'] = cast_node.xpath('//DeliveryHour/text()'.lower())[0]
                EnergyMW = cast_node.xpath('//EnergyMW'.lower())
                dict['EnergyMW'] = get_node_value(EnergyMW)
                dict['SystemName'] = 'Demand_%s_EmbeddedForecast_%s' % (SystemName_value, FuelType_value)
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
        ORRequirement=s_node.xpath('//ORRequirements/ORRequirement'.lower())
        for orrequire in ORRequirement:
            orrequire_node = etree.HTML(etree.tostring(orrequire))
            dict['SystemName'] = 'Demand_%s_ORRequirement' % SystemName_value
            dict['DeliveryHour'] = orrequire_node.xpath('//DeliveryHour/text()'.lower())[0]
            EnergyMW = orrequire_node.xpath('//EnergyMW'.lower())
            dict['EnergyMW'] = get_node_value(EnergyMW)
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        TotalRequirement=s_node.xpath('//TotalRequirements/TotalRequirement'.lower())
        for totalrquire in TotalRequirement:
            totalrquire_node = etree.HTML(etree.tostring(totalrquire))
            dict['SystemName'] = 'Demand_%s_TotalRequirement' % SystemName_value
            dict['DeliveryHour'] = totalrquire_node.xpath('//DeliveryHour/text()'.lower())[0]
            EnergyMW = totalrquire_node.xpath('//EnergyMW'.lower())
            dict['EnergyMW'] = get_node_value(EnergyMW)
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        InternalResource=s_node.xpath('//InternalResources/InternalResource'.lower())
        for iresource in InternalResource:
            iresource_node = etree.HTML(etree.tostring(iresource))
            FuelType_value = iresource_node.xpath('//FuelType/text()'.lower())[0]
            Offered=iresource_node.xpath('//FuelOffered/Offered'.lower())
            for offer in Offered:
                offer_node= etree.HTML(etree.tostring(offer))
                dict['SystemName'] = 'Supply_%s_InternalResource_%s_Offered' % (SystemName_value, FuelType_value)
                dict['DeliveryHour'] = offer_node.xpath('//DeliveryHour/text()'.lower())[0]
                EnergyMW = offer_node.xpath('//EnergyMW'.lower())
                dict['EnergyMW']=get_node_value(EnergyMW)
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
            Scheduled = iresource_node.xpath('//FuelScheduled/Scheduled'.lower())
            for schedule in Scheduled:
                schedule_node = etree.HTML(etree.tostring(schedule))
                dict['SystemName'] = 'Supply_%s_InternalResource_%s_Scheduled' % (SystemName_value, FuelType_value)
                dict['DeliveryHour'] = schedule_node.xpath('//DeliveryHour/text()'.lower())[0]
                EnergyMW = schedule_node.xpath('//EnergyMW'.lower())
                dict['EnergyMW'] = get_node_value(EnergyMW)
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
        ZonalEnergies = s_node.xpath('//IntertieZonalEnergies/ZonalEnergies'.lower())
        for zone in ZonalEnergies:
            zone_node=etree.HTML(etree.tostring(zone))
            IntertieZoneName_value= zone_node.xpath('//IntertieZoneName/text()'.lower())[0]
            Offered=zone_node.xpath('//IntertieZoneImportOffered/Offered'.lower())
            for offer in Offered:
                offer_node= etree.HTML(etree.tostring(offer))
                dict['SystemName'] = 'Supply_%s_Imports_%s_Offered' % (SystemName_value, IntertieZoneName_value)
                dict['DeliveryHour'] = offer_node.xpath('//DeliveryHour/text()'.lower())[0]
                EnergyMW = offer_node.xpath('//EnergyMW'.lower())
                dict['EnergyMW'] = get_node_value(EnergyMW)
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
            Scheduled = zone_node.xpath('//IntertieZoneImportScheduled/Scheduled'.lower())
            for schedule in Scheduled:
                schedule_node = etree.HTML(etree.tostring(schedule))
                dict['SystemName'] = 'Supply_%s_Imports_%s_Scheduled' % (SystemName_value, IntertieZoneName_value)
                dict['DeliveryHour'] = schedule_node.xpath('//DeliveryHour/text()'.lower())[0]
                EnergyMW = schedule_node.xpath('//EnergyMW'.lower())
                dict['EnergyMW'] = get_node_value(EnergyMW)
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
            Bid= zone_node.xpath('//IntertieZoneExportBid/Bid'.lower())
            for bd in Bid:
                bd_node = etree.HTML(etree.tostring(bd))
                dict['SystemName'] = 'Demand_%s_Exports_%s_Bid' % (SystemName_value, IntertieZoneName_value)
                dict['DeliveryHour'] = bd_node.xpath('//DeliveryHour/text()'.lower())[0]
                EnergyMW = bd_node.xpath('//EnergyMW'.lower())
                dict['EnergyMW'] = get_node_value(EnergyMW)
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
            Scheduled = zone_node.xpath('//IntertieZoneExportScheduled/Scheduled'.lower())
            for schedule in Scheduled:
                schedule_node = etree.HTML(etree.tostring(schedule))
                dict['SystemName'] = 'Demand_%s_Exports_%s_Scheduled' % (SystemName_value, IntertieZoneName_value)
                dict['DeliveryHour'] = schedule_node.xpath('//DeliveryHour/text()'.lower())[0]
                EnergyMW = schedule_node.xpath('//EnergyMW'.lower())
                dict['EnergyMW'] = get_node_value(EnergyMW)
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
        UnscheduledResource=s_node.xpath('//UnscheduledResources/UnscheduledResource'.lower())
        for resource in UnscheduledResource:
            resource_node=etree.HTML(etree.tostring(resource))
            dict['SystemName'] = '%s_UnscheduledResource' % SystemName_value
            dict['DeliveryHour'] = resource_node.xpath('//DeliveryHour/text()'.lower())[0]
            EnergyMW = resource_node.xpath('//EnergyMW'.lower())
            dict['EnergyMW'] = get_node_value(EnergyMW)
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        UnscheduledImport=s_node.xpath('//UnscheduledImports/UnscheduledImport'.lower())
        for uimp in UnscheduledImport:
            uimp_node=etree.HTML(etree.tostring(uimp))
            dict['SystemName'] = '%s_UnscheduledImport' % SystemName_value
            dict['DeliveryHour'] = uimp_node.xpath('//DeliveryHour/text()'.lower())[0]
            EnergyMW = uimp_node.xpath('//EnergyMW'.lower())
            dict['EnergyMW'] = get_node_value(EnergyMW)
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        ExcessCapacity=s_node.xpath('//ExcessCapacities/ExcessCapacity'.lower())
        for cap in ExcessCapacity:
            cap_node = etree.HTML(etree.tostring(cap))
            dict['SystemName'] = '%s_ExcessCapacity' % SystemName_value
            dict['DeliveryHour'] = cap_node.xpath('//DeliveryHour/text()'.lower())[0]
            EnergyMW = cap_node.xpath('//EnergyMW'.lower())
            dict['EnergyMW'] = get_node_value(EnergyMW)
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        SupplyCushion=s_node.xpath('//SupplyCushions/SupplyCushion'.lower())
        for cushion in SupplyCushion:
            cushion_node = etree.HTML(etree.tostring(cushion))
            dict['SystemName'] = '%s_SupplyCushion' % SystemName_value
            dict['DeliveryHour'] = cushion_node.xpath('//DeliveryHour/text()'.lower())[0]
            EnergyMW = cushion_node.xpath('//EnergyMW'.lower())
            dict['EnergyMW'] = get_node_value(EnergyMW)
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

xml_folder = '/home/peak/Dropbox (Peak Power Inc)/IESO/IESO_Organized/2016/System Adequacy/'
csv_folder = '/home/peak/IESO-CSV/2016/System Adequacy/'

def generate_list_filename_v1(startdate, enddate, folder):
    dayList = pd.date_range(startdate, enddate, freq='D')
    dayListStr = []
    # PUB_Adequacy_20160101_v1
    for day in dayList:
        daystr = str(day).split(' ')[0].split('-')
        for i in np.arange(1, 34, 1):
            dayListStr.append('%sPUB_Adequacy_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
    return dayListStr

def IsSubString(subList, str):
    flag = True
    for subStr in subList:
        if not (subStr in str):
            flag = False
    return flag


# get all the files name from folder,filter by FlagStr
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

def save_csv(filename):
    t1 = datetime.datetime.now()
    df = get_DataFrame_SystemAdequacy_v1(filename)
    hour = (filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (csv_folder, hour), header=True)
    t2 = datetime.datetime.now()
    print 'saved:%s,%s seconds' % (hour, t2 - t1)
    time.sleep(2)
    return filename

def year_xml2df_systemadequacy(file_folder):
    gen_file_list = generate_list_filename_v1('20150101', '20151231', file_folder)
    get_list_file = get_list_filename(file_folder, ['.xml'])
    print len(get_list_file)
    used_list = []
    for gen_str in gen_file_list:
        flag = True
        for get_str in get_list_file:
            if gen_str == get_str:
                flag = False
        if flag == False:
            used_list.append(gen_str)
            # if flag:
            # print gen_str
    print '-----------------%i-------------------------' % len(used_list)

    # save_csv(used_list[0])
    t1 = datetime.datetime.now()
    print t1
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    pool.map(save_csv, used_list)
    t2 = datetime.datetime.now()
    print t2 - t1