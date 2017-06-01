from lxml import etree
import pandas as pd
import datetime, time
import multiprocessing
import numpy as np

# file_path='C:/Users/benson/Desktop/2016-example/PUB_Adequacy_20160101_v1.xml'
file_path = 'C:/Users/benson/Downloads/Adequacy Report.xml'


# PUB_Adequacy_20160101_v1
def get_node_value(node):
    if len(node) > 0:
        return node[0].text
    else:
        return -1


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
    path0 = '//DocBody/System[%i]'
    for i in range(len(System)):
        SystemName = html.xpath(((path0 + '/SystemName') % (i + 1)).lower())
        SystemName_value = SystemName[0].text
        # --//DocBody/System/Demands/PeakDemand/Demand
        Demand = html.xpath(((path0 + '/Demands/PeakDemand/Demand') % (i + 1)).lower())
        path1 = path0 + '/Demands/PeakDemand/Demand[%i]'
        for j in range(len(Demand)):
            dict['SystemName'] = '%s_PeakDemand' % SystemName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # --//DocBody/System/Demands/AverageDemand/Demand
        Demand = html.xpath(((path0 + '/Demands/AverageDemand/Demand') % (i + 1)).lower())
        path1 = path0 + '/Demands/AverageDemand/Demand[%i]'
        for j in range(len(Demand)):
            dict['SystemName'] = 'Demand_%s_AverageDemand' % SystemName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # --//DocBody/System/EmbeddedForecasts/EmbeddedForecast/
        EmbeddedForecast = html.xpath(((path0 + '/EmbeddedForecasts/EmbeddedForecast') % (i + 1)).lower())
        path1 = path0 + '/EmbeddedForecasts/EmbeddedForecast[%i]'
        for j in range(len(EmbeddedForecast)):
            FuelType = html.xpath(((path1 + '/FuelType') % (i + 1, j + 1)).lower())
            fuelType_value = FuelType[0].text
            Forecast = html.xpath(((path1 + '/Forecast') % (i + 1, j + 1)).lower())
            path2 = path1 + '/Forecast[%i]'
            for k in range(len(Forecast)):
                dict['SystemName'] = 'Demand_%s_EmbeddedForecast_%s' % (SystemName_value, fuelType_value)
                DeliveryHour = html.xpath(((path2 + '/DeliveryHour') % (i + 1, j + 1, k + 1)).lower())
                dict['DeliveryHour'] = DeliveryHour[0].text
                EnergyMW = html.xpath(((path2 + '/EnergyMW') % (i + 1, j + 1, k + 1)).lower())
                dict['EnergyMW'] = EnergyMW[0].text
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
        # --//DocBody/System/ORRequirements/ORRequirement
        ORRequirement = html.xpath(((path0 + '/ORRequirements/ORRequirement') % (i + 1)).lower())
        path1 = path0 + '/ORRequirements/ORRequirement[%i]'
        for j in range(len(ORRequirement)):
            dict['SystemName'] = 'Demand_%s_ORRequirement' % SystemName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # --//DocBody/System/TotalRequirements/TotalRequirement
        TotalRequirement = html.xpath(((path0 + '/TotalRequirements/TotalRequirement') % (i + 1)).lower())
        path1 = path0 + '/TotalRequirements/TotalRequirement[%i]'
        for j in range(len(TotalRequirement)):
            dict['SystemName'] = 'Demand_%s_TotalRequirement' % SystemName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # --//DocBody/System/InternalResources
        InternalResource = html.xpath(((path0 + '/InternalResources/InternalResource') % (i + 1)).lower())
        path1 = path0 + '/InternalResources/InternalResource[%i]'
        for j in range(len(InternalResource)):
            FuelType = html.xpath(((path1 + '/FuelType') % (i + 1, j + 1)).lower())
            FuelType_value = FuelType[0].text
            Offered = html.xpath(((path1 + '/FuelOffered/Offered') % (i + 1, j + 1)).lower())
            path2 = path1 + '/FuelOffered/Offered[%i]'
            for k in range(len(Offered)):
                dict['SystemName'] = 'Supply_%s_InternalResource_%s_Offered' % (SystemName_value, fuelType_value)
                DeliveryHour = html.xpath(((path2 + '/DeliveryHour') % (i + 1, j + 1, k + 1)).lower())
                dict['DeliveryHour'] = DeliveryHour[0].text
                EnergyMW = html.xpath(((path2 + '/EnergyMW') % (i + 1, j + 1, k + 1)).lower())
                dict['EnergyMW'] = EnergyMW[0].text
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
            # FuelScheduled
            Scheduled = html.xpath(((path1 + '/FuelScheduled/Scheduled') % (i + 1, j + 1)).lower())
            path2 = path1 + '/FuelScheduled/Scheduled[%i]'
            for k in range(len(Scheduled)):
                dict['SystemName'] = 'Supply_%s_InternalResource_%s_Scheduled' % (SystemName_value, fuelType_value)
                DeliveryHour = html.xpath(((path2 + '/DeliveryHour') % (i + 1, j + 1, k + 1)).lower())
                dict['DeliveryHour'] = DeliveryHour[0].text
                EnergyMW = html.xpath(((path2 + '/EnergyMW') % (i + 1, j + 1, k + 1)).lower())
                dict['EnergyMW'] = EnergyMW[0].text
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
        # --//DocBody/System/IntertieZonalEnergies
        ZonalEnergies = html.xpath(((path0 + '/IntertieZonalEnergies/ZonalEnergies') % (i + 1)).lower())
        path1 = path0 + '/IntertieZonalEnergies/ZonalEnergies[%i]'
        for j in range(len(ZonalEnergies)):
            IntertieZoneName = html.xpath(((path1 + '/IntertieZoneName') % (i + 1, j + 1)).lower())
            IntertieZoneName_value = IntertieZoneName[0].text
            # IntertieZoneImportOffered
            Offered = html.xpath(((path1 + '/IntertieZoneImportOffered/Offered') % (i + 1, j + 1)).lower())
            path2 = path1 + '/IntertieZoneImportOffered/Offered[%i]'
            for k in range(len(Offered)):
                dict['SystemName'] = 'Supply_%s_Imports_%s_Offered' % (SystemName_value, IntertieZoneName_value)
                DeliveryHour = html.xpath(((path2 + '/DeliveryHour') % (i + 1, j + 1, k + 1)).lower())
                dict['DeliveryHour'] = DeliveryHour[0].text
                EnergyMW = html.xpath(((path2 + '/EnergyMW') % (i + 1, j + 1, k + 1)).lower())
                dict['EnergyMW'] = EnergyMW[0].text
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
            # IntertieZoneImportScheduled
            Scheduled = html.xpath(((path1 + '/IntertieZoneImportScheduled/Scheduled') % (i + 1, j + 1)).lower())
            path2 = path1 + '/IntertieZoneImportScheduled/Scheduled[%i]'
            for k in range(len(Scheduled)):
                dict['SystemName'] = 'Supply_%s_Imports_%s_Scheduled' % (SystemName_value, IntertieZoneName_value)
                DeliveryHour = html.xpath(((path2 + '/DeliveryHour') % (i + 1, j + 1, k + 1)).lower())
                dict['DeliveryHour'] = DeliveryHour[0].text
                EnergyMW = html.xpath(((path2 + '/EnergyMW') % (i + 1, j + 1, k + 1)).lower())
                dict['EnergyMW'] = EnergyMW[0].text
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
            # IntertieZoneExportBid
            Bid = html.xpath(((path1 + '/IntertieZoneExportBid/Bid') % (i + 1, j + 1)).lower())
            path2 = path1 + '/IntertieZoneExportBid/Bid[%i]'
            for k in range(len(Bid)):
                dict['SystemName'] = 'Demand_%s_Exports_%s_Bid' % (SystemName_value, IntertieZoneName_value)
                DeliveryHour = html.xpath(((path2 + '/DeliveryHour') % (i + 1, j + 1, k + 1)).lower())
                dict['DeliveryHour'] = DeliveryHour[0].text
                EnergyMW = html.xpath(((path2 + '/EnergyMW') % (i + 1, j + 1, k + 1)).lower())
                dict['EnergyMW'] = EnergyMW[0].text
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
            # IntertieZoneExportScheduled
            Scheduled = html.xpath(((path1 + '/IntertieZoneExportScheduled/Scheduled') % (i + 1, j + 1)).lower())
            path2 = path1 + '/IntertieZoneExportScheduled/Scheduled[%i]'
            for k in range(len(Scheduled)):
                dict['SystemName'] = 'Demand_%s_Exports_%s_Scheduled' % (SystemName_value, IntertieZoneName_value)
                DeliveryHour = html.xpath(((path2 + '/DeliveryHour') % (i + 1, j + 1, k + 1)).lower())
                dict['DeliveryHour'] = DeliveryHour[0].text
                EnergyMW = html.xpath(((path2 + '/EnergyMW') % (i + 1, j + 1, k + 1)).lower())
                dict['EnergyMW'] = EnergyMW[0].text
                timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
                dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
                dict2 = {}
                dict2.update(dict)
                data_list.append(dict2)
        # --//DocBody/System/UnscheduledResources
        UnscheduledResource = html.xpath(((path0 + '/UnscheduledResources/UnscheduledResource') % (i + 1)).lower())
        path1 = path0 + '/UnscheduledResources/UnscheduledResource[%i]'
        for j in range(len(UnscheduledResource)):
            dict['SystemName'] = '%s_UnscheduledResource' % SystemName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # --//DocBody/System/UnscheduledImports
        UnscheduledImport = html.xpath(((path0 + '/UnscheduledImports/UnscheduledImport') % (i + 1)).lower())
        path1 = path0 + '/UnscheduledImports/UnscheduledImport[%i]'
        for j in range(len(UnscheduledImport)):
            dict['SystemName'] = '%s_UnscheduledImport' % SystemName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # --//DocBody/System/ExcessCapacities
        ExcessCapacity = html.xpath(((path0 + '/ExcessCapacities/ExcessCapacity') % (i + 1)).lower())
        path1 = path0 + '/ExcessCapacities/ExcessCapacity[%i]'
        for j in range(len(ExcessCapacity)):
            dict['SystemName'] = '%s_ExcessCapacity' % SystemName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # --//DocBody/System/SupplyCushions
        SupplyCushion = html.xpath(((path0 + '/SupplyCushions/SupplyCushion') % (i + 1)).lower())
        path1 = path0 + '/SupplyCushions/SupplyCushion[%i]'
        for j in range(len(SupplyCushion)):
            dict['SystemName'] = '%s_SupplyCushion' % SystemName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)


def get_DataFrame_SystemAdequacy_v2(filepath):
    doc = etree.parse(filepath)
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
    # --ForecastSupply/Capacities
    Capacity = html.xpath('//ForecastSupply/Capacities/Capacity'.lower())
    path0 = '//ForecastSupply/Capacities/Capacity[%i]'
    for i in range(len(Capacity)):
        dict['SystemName'] = 'Supply_Capacity'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # --ForecastSupply/Energies
    Energy = html.xpath('//ForecastSupply/Energies/Energy'.lower())
    path0 = '//ForecastSupply/Energies/Energy[%i]'
    for i in range(len(Energy)):
        dict['SystemName'] = 'Supply_Energy'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = 0
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # --ForecastSupply/InternalResources
    InternalResource = html.xpath('//ForecastSupply/InternalResources/InternalResource'.lower())
    path0 = '//ForecastSupply/InternalResources/InternalResource[%i]'
    for i in range(len(InternalResource)):
        FuelType = html.xpath(((path0 + '/FuelType') % (i + 1)).lower())
        FuelType_value = FuelType[0].text
        # Capacities
        Capacity = html.xpath(((path0 + '/Capacities/Capacity') % (i + 1)).lower())
        path1 = path0 + '/Capacities/Capacity[%i]'
        for j in range(len(Capacity)):
            dict['SystemName'] = 'Supply_%s_Capacity' % FuelType_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # Outages
        Outage = html.xpath(((path0 + '/Outages/Outage') % (i + 1)).lower())
        path1 = path0 + '/Outages/Outage[%i]'
        for j in range(len(Outage)):
            dict['SystemName'] = 'Supply_%s_Outage' % FuelType_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # Offers
        Offer = html.xpath(((path0 + '/Offers/Offer') % (i + 1)).lower())
        path1 = path0 + '/Offers/Offer[%i]'
        for j in range(len(Offer)):
            dict['SystemName'] = 'Supply_%s_Offer' % FuelType_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # Schedules
        Schedule = html.xpath(((path0 + '/Schedules/Schedule') % (i + 1)).lower())
        path1 = path0 + '/Schedules/Schedule[%i]'
        for j in range(len(Schedule)):
            dict['SystemName'] = 'Supply_%s_Schedule' % FuelType_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    # --ForecastSupply/ZonalImports
    ZonalImport = html.xpath('//ForecastSupply/ZonalImports/ZonalImport'.lower())
    path0 = '//ForecastSupply/ZonalImports/ZonalImport[%i]'
    for i in range(len(ZonalImport)):
        ZoneName = html.xpath(((path0 + '/ZoneName') % (i + 1)).lower())
        ZoneName_value = ZoneName[0].text
        # offers
        Offer = html.xpath(((path0 + '/Offers/Offer') % (i + 1)).lower())
        path1 = path0 + '/Offers/Offer[%i]'
        for j in range(len(Offer)):
            dict['SystemName'] = 'ZonalImports_%s_Offer' % ZoneName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            if len(EnergyMW) > 0:
                dict['EnergyMW'] = EnergyMW[0].text
            else:
                dict['EnergyMW'] = -1
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # Schedules
        Schedule = html.xpath(((path0 + '/Schedules/Schedule') % (i + 1)).lower())
        path1 = path0 + '/Schedules/Schedule[%i]'
        for j in range(len(Schedule)):
            dict['SystemName'] = 'ZonalImports_%s_Schedule' % ZoneName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            dict['EnergyMW'] = EnergyMW[0].text
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    # --ForecastSupply/BottledCapacities
    Capacity = html.xpath('//ForecastSupply/BottledCapacities/Capacity'.lower())
    path0 = '//ForecastSupply/BottledCapacities/Capacity[%i]'
    for i in range(len(Capacity)):
        dict['SystemName'] = 'Supply_BottledCapacity'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # --ForecastSupply/TotalSupplies
    Supply = html.xpath('//ForecastSupply/TotalSupplies/Supply'.lower())
    path0 = '//ForecastSupply/TotalSupplies/Supply[%i]'
    for i in range(len(Supply)):
        dict['SystemName'] = 'Supply_TotalSupply'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)

    # ForecastDemand-OntarioDemand
    # PeakDemand
    Demand = html.xpath('//ForecastDemand/OntarioDemand/PeakDemand/Demand'.lower())
    path0 = '//ForecastDemand/OntarioDemand/PeakDemand/Demand[%i]'
    for i in range(len(Demand)):
        dict['SystemName'] = 'Demand_Ontario_Peak'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # AverageDemand
    Demand = html.xpath('//ForecastDemand/OntarioDemand/AverageDemand/Demand'.lower())
    path0 = '//ForecastDemand/OntarioDemand/AverageDemand/Demand[%i]'
    for i in range(len(Demand)):
        dict['SystemName'] = 'Demand_Ontario_Average'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # WindEmbedded
    Embedded = html.xpath('//ForecastDemand/OntarioDemand/WindEmbedded/Embedded'.lower())
    path0 = '//ForecastDemand/OntarioDemand/WindEmbedded/Embedded[%i]'
    for i in range(len(Embedded)):
        dict['SystemName'] = 'Demand_Ontario_WindEmbedded'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # SolarEmbedded
    Embedded = html.xpath('//ForecastDemand/OntarioDemand/SolarEmbedded/Embedded'.lower())
    path0 = '//ForecastDemand/OntarioDemand/SolarEmbedded/Embedded[%i]'
    for i in range(len(Embedded)):
        dict['SystemName'] = 'Demand_Ontario_SolarEmbedded'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # DispatchableLoad-Capacity
    Capacity = html.xpath('//ForecastDemand/OntarioDemand/DispatchableLoad/Capacities/Capacity'.lower())
    path0 = '//ForecastDemand/OntarioDemand/DispatchableLoad/Capacities/Capacity[%i]'
    for i in range(len(Capacity)):
        dict['SystemName'] = 'Demand_Ontario_DispatchableLoad_Capacity'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # DispatchableLoad-BidForecast
    BidForecast = html.xpath('//ForecastDemand/OntarioDemand/DispatchableLoad/BidForecasts/BidForecast'.lower())
    path0 = '//ForecastDemand/OntarioDemand/DispatchableLoad/BidForecasts/BidForecast[%i]'
    for i in range(len(BidForecast)):
        dict['SystemName'] = 'Demand_Ontario_DispatchableLoad_BidForecast'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # DispatchableLoad-ScheduledON
    Schedule = html.xpath('//ForecastDemand/OntarioDemand/DispatchableLoad/ScheduledON/Schedule'.lower())
    path0 = '//ForecastDemand/OntarioDemand/DispatchableLoad/ScheduledON/Schedule[%i]'
    for i in range(len(Schedule)):
        dict['SystemName'] = 'Demand_Ontario_DispatchableLoad_ScheduledON'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # DispatchableLoad-ScheduledOFF
    Schedule = html.xpath('//ForecastDemand/OntarioDemand/DispatchableLoad/ScheduledOFF/Schedule'.lower())
    path0 = '//ForecastDemand/OntarioDemand/DispatchableLoad/ScheduledOFF/Schedule[%i]'
    for i in range(len(Schedule)):
        dict['SystemName'] = 'Demand_Ontario_DispatchableLoad_ScheduledOFF'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # HourlyDemandResponse-Bids
    Bid = html.xpath('//ForecastDemand/OntarioDemand/HourlyDemandResponse/Bids/Bid'.lower())
    path0 = '//ForecastDemand/OntarioDemand/HourlyDemandResponse/Bids/Bid[%i]'
    for i in range(len(Bid)):
        dict['SystemName'] = 'Demand_Ontario_HourlyDemandResponse_Bid'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # HourlyDemandResponse-Bids
    Schedule = html.xpath('//ForecastDemand/OntarioDemand/HourlyDemandResponse/Schedules/Schedule'.lower())
    path0 = '//ForecastDemand/OntarioDemand/HourlyDemandResponse/Schedules/Schedule[%i]'
    for i in range(len(Schedule)):
        dict['SystemName'] = 'Demand_Ontario_HourlyDemandResponse_Schedule'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # HourlyDemandResponse-Curtailed
    Curtail = html.xpath('//ForecastDemand/OntarioDemand/HourlyDemandResponse/Curtailed/Curtail'.lower())
    path0 = '//ForecastDemand/OntarioDemand/HourlyDemandResponse/Curtailed/Curtail[%i]'
    for i in range(len(Curtail)):
        dict['SystemName'] = 'Demand_Ontario_HourlyDemandResponse_Curtail'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # ZonalExports

    ZonalExport = html.xpath('//ForecastDemand/ZonalExports/ZonalExport'.lower())
    path0 = '//ForecastDemand/ZonalExports/ZonalExport[%i]'
    for i in range(len(ZonalExport)):
        ZoneName = html.xpath(((path0 + '/ZoneName') % (i + 1)).lower())
        ZoneName_value = ZoneName[0].text
        # Bids
        Bid = html.xpath(((path0 + '/Bids/Bid') % (i + 1)).lower())
        path1 = path0 + '/Bids/Bid[%i]'
        for j in range(len(Bid)):
            dict['SystemName'] = 'Demand_%s_ZonalExports_Bid' % ZoneName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            if len(EnergyMW) > 0:
                dict['EnergyMW'] = EnergyMW[0].text
            else:
                dict['EnergyMW'] = -1
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
        # Schedules
        Schedule = html.xpath(((path0 + '/Schedules/Schedule') % (i + 1)).lower())
        path1 = path0 + '/Schedules/Schedule[%i]'
        for j in range(len(Schedule)):
            dict['SystemName'] = 'Demand_%s_ZonalExports_Schedule' % ZoneName_value
            DeliveryHour = html.xpath(((path1 + '/DeliveryHour') % (i + 1, j + 1)).lower())
            dict['DeliveryHour'] = DeliveryHour[0].text
            EnergyMW = html.xpath(((path1 + '/EnergyMW') % (i + 1, j + 1)).lower())
            if len(EnergyMW) > 0:
                dict['EnergyMW'] = EnergyMW[0].text
            else:
                dict['EnergyMW'] = -1
            timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    # --GenerationReserveHoldback/TotalORReserve/
    ORReserve = html.xpath('//ForecastDemand/GenerationReserveHoldback/TotalORReserve/ORReserve'.lower())
    path0 = '//ForecastDemand/GenerationReserveHoldback/TotalORReserve/ORReserve[%i]'
    for i in range(len(ORReserve)):
        dict['SystemName'] = 'Demand_GenerationReserveHoldback_ORReserve'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # GenerationReserveHoldback-Min10MinOR
    Min10OR = html.xpath('//ForecastDemand/GenerationReserveHoldback/Min10MinOR/Min10OR'.lower())
    path0 = '//ForecastDemand/GenerationReserveHoldback/Min10MinOR/Min10OR[%i]'
    for i in range(len(Min10OR)):
        dict['SystemName'] = 'Demand_GenerationReserveHoldback_Min10OR'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # GenerationReserveHoldback-Min10MinSpinOR
    Min10SpinOR = html.xpath('//ForecastDemand/GenerationReserveHoldback/Min10MinSpinOR/Min10SpinOR'.lower())
    path0 = '//ForecastDemand/GenerationReserveHoldback/Min10MinSpinOR/Min10SpinOR[%i]'
    for i in range(len(Min10SpinOR)):
        dict['SystemName'] = 'Demand_GenerationReserveHoldback_Min10SpinOR'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # GenerationReserveHoldback-LoadForecastUncertainties
    Uncertainty = html.xpath('//ForecastDemand/GenerationReserveHoldback/LoadForecastUncertainties/Uncertainty'.lower())
    path0 = '//ForecastDemand/GenerationReserveHoldback/LoadForecastUncertainties/Uncertainty[%i]'
    for i in range(len(Uncertainty)):
        dict['SystemName'] = 'Demand_GenerationReserveHoldback_Uncertainty'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # GenerationReserveHoldback-ContingencyAllowances
    Allowance = html.xpath('//ForecastDemand/GenerationReserveHoldback/ContingencyAllowances/Allowance'.lower())
    path0 = '//ForecastDemand/GenerationReserveHoldback/ContingencyAllowances/Allowance[%i]'
    for i in range(len(Allowance)):
        dict['SystemName'] = 'Demand_GenerationReserveHoldback_Allowance'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # TotalRequirements
    Requirement = html.xpath('//ForecastDemand/TotalRequirements/Requirement'.lower())
    path0 = '//ForecastDemand/TotalRequirements/Requirement[%i]'
    for i in range(len(Requirement)):
        dict['SystemName'] = 'Demand_TotalRequirements_Requirement'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # ExcessCapacities
    Capacity = html.xpath('//ForecastDemand/ExcessCapacities/Capacity'.lower())
    path0 = '//ForecastDemand/ExcessCapacities/Capacity[%i]'
    for i in range(len(Capacity)):
        dict['SystemName'] = 'Demand_ExcessCapacities_Capacity'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        dict['EnergyMW'] = EnergyMW[0].text
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # ExcessEnergies
    Energy = html.xpath('//ForecastDemand/ExcessEnergies/Energy'.lower())
    path0 = '//ForecastDemand/ExcessEnergies/Energy[%i]'
    for i in range(len(Energy)):
        dict['SystemName'] = 'Demand_ExcessEnergies_Energy'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # ExcessOfferedCapacities
    Capacity = html.xpath('//ForecastDemand/ExcessOfferedCapacities/Capacity'.lower())
    path0 = '//ForecastDemand/ExcessOfferedCapacities/Capacity[%i]'
    for i in range(len(Capacity)):
        dict['SystemName'] = 'Demand_ExcessOfferedCapacities_Capacity'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # UnscheduledResources
    UnscheduledResource = html.xpath('//ForecastDemand/UnscheduledResources/UnscheduledResource'.lower())
    path0 = '//ForecastDemand/UnscheduledResources/UnscheduledResource[%i]'
    for i in range(len(UnscheduledResource)):
        dict['SystemName'] = 'Demand_UnscheduledResources_UnscheduledResource'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    # UnscheduledImports
    UnscheduledImport = html.xpath('//ForecastDemand/UnscheduledImports/UnscheduledImport'.lower())
    path0 = '//ForecastDemand/UnscheduledImports/UnscheduledImport[%i]'
    for i in range(len(UnscheduledImport)):
        dict['SystemName'] = 'Demand_UnscheduledImports_UnscheduledImport'
        DeliveryHour = html.xpath(((path0 + '/DeliveryHour') % (i + 1)).lower())
        dict['DeliveryHour'] = DeliveryHour[0].text
        EnergyMW = html.xpath(((path0 + '/EnergyMW') % (i + 1)).lower())
        if len(EnergyMW) > 0:
            dict['EnergyMW'] = EnergyMW[0].text
        else:
            dict['EnergyMW'] = -1
        timestr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
        dict['datetime'] = datetime.datetime.strptime(timestr, '%Y-%m-%d-%H')
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)


file_folder = '/home/peak/Dropbox (Peak Power Inc)/IESO/IESO_Organized/2016/System Adequacy/'
save_folder = '/home/peak/IESO-CSV/2016/System Adequacy/'


# C:\Users\benson\Desktop\2016\Predispatch Shadow Prices Report

# benson writed the code here
# generate all the fileList
def generate_list_filename_v1(startdate, enddate, folder):
    dayList = pd.date_range(startdate, enddate, freq='D')
    dayListStr = []
    # PUB_Adequacy_20160101_v1
    for day in dayList:
        daystr = str(day).split(' ')[0].split('-')
        for i in np.arange(1, 34, 1):
            dayListStr.append('%sPUB_Adequacy_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
    return dayListStr


def generate_list_filename_v2(startdate, enddate, folder):
    dayList = pd.date_range(startdate, enddate, freq='D')
    dayListStr = []
    # PUB_Adequacy2_20170207_v1
    for day in dayList:
        daystr = str(day).split(' ')[0].split('-')
        for i in np.arange(1, 126, 1):
            dayListStr.append('%sPUB_Adequacy2_%s%s%s_v%i.xml' % (folder, daystr[0], daystr[1], daystr[2], i))
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
    df = get_DataFrame_SystemAdequacy_v2(filename)
    hour = (filename.split('/')[-1]).split('.')[0]
    df.to_csv('%s%s.csv' % (save_folder, hour), header=True)
    t2 = datetime.datetime.now()
    print 'saved:%s,%s seconds' % (hour, t2 - t1)
    time.sleep(2)
    return filename


def print_result(request, result):
    print "result:%s %r" % (request.requestID, result)


def year_xml2df_systemadequacy(file_folder):
    gen_file_list = generate_list_filename_v2('20160629', '20170207', file_folder)
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


test_integrate_folder = '/home/peak/IESO-CSV/2016/System Adequacy'
test_integrate_file_v2 = 'C:/Users/benson/Desktop/2016-example/PUB_Adequacy_20160101_v1.csv'
test_integrate_file_v1 = 'C:/Users/benson/Desktop/2016-example/PUB_Adequacy_20160101_v1.csv'

# PUB_Adequacy_20160103_v
# 2016-01-01T23:18:51
file_folder='C:/Users/benson/Desktop/IESO/2016/System Adequacy/'
save_folder='C:/Users/benson/Desktop/day_data/2016/System Adequacy/'
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

def IsSubString(subList, str):
    flag = True
    for subStr in subList:
        if not (subStr in str):
            flag = False
    return flag

def get_csv_list(daystr,folder):
    f_list=get_list_filename(folder,['.csv'])
    day_list=[]
    for file in f_list:
        if file.find(daystr)>=0:
            day_list.append(file)
    return day_list

def generate_SystemAdequacy_Table(dayStr,header,hours_forecast):
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
    csv_list=get_csv_list(daystr,file_folder)
    df = pd.read_csv(csv_list[0])
    headers=df.groupby(df['SystemName'])
    head_list=[]
    for head in headers:
       head_list.append(head[0])
    df_save=generate_SystemAdequacy_Table(daystr,head_list,8)
    # --create a save data table--
    for file in csv_list:
        df=pd.read_csv(file)
        for index in df.index:
            str_name = df.loc[index, ['SystemName']][0]
            ctime = df.loc[index, ['CreatedAt']][0]
            ctime_y = datetime.datetime.strptime(ctime, '%Y-%m-%dT%H:%M:%S')
            dtime = df.loc[index, ['datetime']][0]
            dtime_y = datetime.datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
            value=df.loc[index, ['EnergyMW']][0]
            df_save=update_dataframe_value(dtime_y,ctime_y,str_name,value,df_save)
    df_save.to_csv('%sPUB_Adequacy2_%s.csv' % (save_folder,daystr))
    t2=datetime.datetime.now()
    print 'saved:%s'%(t2-t1)


t1=datetime.datetime.now()
print t1
day_list=pd.date_range('2016-01-01 00:00:00','2016-12-31 00:00:00',freq='D')
pool=multiprocessing.Pool(multiprocessing.cpu_count())
pool.map(time_index_dataframe,day_list)
t2=datetime.datetime.now()
print t2-t1
