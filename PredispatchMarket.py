from lxml import etree
import pandas as pd
import datetime,time
import  multiprocessing

file_path='C:/Users/benson/Desktop/2016-example/PUB_PredispMktTotals_20160101_v1.xml'
def get_DataFrame_PredispatchMarket(filePath):
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
    HourlyUnConstrainedEnergy=html.xpath('//Energies/HourlyUnConstrainedEnergy'.lower())
    for energy in HourlyUnConstrainedEnergy:
        node = etree.HTML(etree.tostring(energy))
        dict['DeliveryHour']=node.xpath('//DeliveryHour/text()'.lower())[0]
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
            timeStr = dict['DeliveryDate'] + '-' + str(int(dict['DeliveryHour']) - 1)
            dict['datetime'] = datetime.datetime.strptime(timeStr, '%Y-%m-%d-%H')
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

df=get_DataFrame_PredispatchMarket(file_path)
print df.shape
print df.head()
