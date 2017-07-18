import xlrd
import pandas as pd
import datetime
import json
import urllib

file_path='C:/Users/benson/Downloads/IESO-Active-Contracted-Generation-List - Mar 2017.xlsx'
google_map_ur='https://maps.googleapis.com/maps/api/geocode/json?address='
api_key='+ON&key=AIzaSyBpKPeE7bKhKDEXlZYNeDOaiEvCliPhqvk'
# AIzaSyBpKPeE7bKhKDEXlZYNeDOaiEvCliPhqvk
# AIzaSyDVImqX-k4zlC-QpzNm_Jf4s2280PK09dE
def minimalist_xldate_as_datetime(xldate, datemode):
    return (
        datetime.datetime(1899, 12, 30)
        + datetime.timedelta(days=(xldate) + 1462 * datemode)
        )

def get_DataFrame_ActiveConGeneration(filePath):
    wb = xlrd.open_workbook(file_path)
    sheet_names=wb.sheet_names()
    print xlrd.xldate.xldate_as_datetime(41109, wb.datemode)
    data_sheet = wb.sheet_by_name(u'Contract Data')
    first_row_output = data_sheet.row_values(2)
    print len(first_row_output)
    print first_row_output
    first_col_output=data_sheet.col_values(0)
    print len(first_col_output)
    data_list = []
    dict = {}

    for j in range(3,len(first_col_output),1):
        for i in range(0, len(first_row_output), 1):
            out_value = data_sheet.cell(j, i).value
            out_title=data_sheet.cell(2, i).value
            if out_title.find('Date')>=0:
                out_value=str(out_value)
                out_value=out_value.split('.')[0]
                if out_value.find('N/A')<0:
                    if out_value.__len__()>0:
                        dict[out_title]=out_value
                        # out_int=int(out_value)
                        # dict[out_title] = minimalist_xldate_as_datetime(out_int,0)
                        # print 'i;%d j:%d time:%s  %s'%(i,j,dict[out_title],out_title)
                    else:
                        dict[out_title] = 'N/A'
                else:
                    dict[out_title]=None
            else:
                dict[out_title]=out_value
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)
# [u'Contract Type', u'Contract Capacity (MW)',
# u'Project Name', u'Supplier Legal Name',
# u'Technology', u'Fuel',
#  u'Contract Status',
# u'Contract Date', u'Milestone Commercial Operation Date',
#  u'Commercial Operation Date', u'Contract Term (Years)',
# u'Connection Type', u'Approximate Location (City/Town)',
# u'IESO Zone']

def get_ContractCapacity_solar(opdate):
    df=get_DataFrame_ActiveConGeneration(file_path)
    df.set_index=range(0,len(df))
    solar_capacity=0
    for i in range(len(df)):
        fuel=df.at[i,'Fuel']
        if fuel.find('Solar')>=0:
            commercial_operation_date=df.at[i,'Commercial Operation Date']
            if commercial_operation_date!='N/A':
                if commercial_operation_date<opdate:
                    energy=df.at[i,'Contract Capacity (MW)']
                    if energy!='N/A':
                        solar_capacity+=energy
    print 'solar_capacity:%.2f'%solar_capacity

def get_Geo(url):
    u = urllib.urlopen(url)
    data=json.load(u)
    results=data['results']
    location_lat_lng=[]
    for r in results:
        geometry=r['geometry']
        location=geometry['location']
        lat=location['lat']
        location_lat_lng.append(lat)
        lng=location['lng']
        location_lat_lng.append(lng)
    return location_lat_lng

def get_solar_dataframe():
    df=get_DataFrame_ActiveConGeneration(file_path)
    df.index=range(len(df))
    df_solar=[]
    contract_solar_cap=0
    for i in range(len(df)):
        str_fuel=df.at[i,'Fuel']
        if str_fuel.find('Solar')>=0:
            contract_status=df.at[i,'Contract Status']
            if contract_status.find('CO')>=0:
                solar_cap=df.at[i,'Contract Capacity (MW)']
                out_value = str(solar_cap)
                if out_value.find('N/A') < 0:
                    contract_solar_cap+=solar_cap
                df_solar.append(df.loc[i])
    df_solar=pd.DataFrame(df_solar)
    print df_solar.shape
    print df_solar.head()
    df_solar.index=range(len(df_solar))
    lat=[]
    lng=[]
    # longitude and latitude
    for i in range(len(df_solar)):
        # Project Name
        # Approximate Location(City / Town)
        name=df_solar.at[i,'Project Name']
        city=df_solar.at[i,'Approximate Location (City/Town)']
        city=city.replace('. ','.')
        # city=urllib.quote(city)
        # print 'city:%s name:%s'%(city,name)

        url='%s%s%s'%(google_map_ur,city,api_key)

        print url
        geo=get_Geo(url)
        if len(geo)>0:
            lat.append(geo[0])
            lng.append(geo[1])
        else:
            lat.append('N/A')
            lng.append('N/A')
    df_solar['latitude']=lat
    df_solar['longitude']=lng
    df_solar.to_csv('C:/Users/benson/Desktop/IESO-ANA/IACG_solar.csv')

def get_wind_dataframe():
    df=get_DataFrame_ActiveConGeneration(file_path)
    df_wind=[]
    contract_wind_cap=0
    for i in range(len(df)):
        str_fuel=df.at[i,'Fuel']
        if str_fuel.find('Wind')>=0:
            contract_status = df.at[i, 'Contract Status']
            if contract_status.find('CO') >= 0:
                wind_cap = df.at[i, 'Contract Capacity (MW)']
                out_value = str(wind_cap)
                if out_value.find('N/A') < 0:
                    contract_wind_cap += wind_cap
                df_wind.append(df.loc[i])
    df_wind=pd.DataFrame(df_wind)
    df_wind.index = range(len(df_wind))
    lat = []
    lng = []
    # longitude and latitude
    for i in range(len(df_wind)):
        # Project Name
        # Approximate Location(City / Town)
        name = df_wind.at[i, 'Project Name']
        city = df_wind.at[i, 'Approximate Location (City/Town)']
        city = city.replace('. ', '.')
        url = '%s%s%s' % (google_map_ur,  city, api_key)
        print city
        print url
        geo = get_Geo(url)
        if len(geo)>0:
            lat.append(geo[0])
            lng.append(geo[1])
        else:
            lat.append('N/A')
            lng.append('N/A')
    df_wind['latitude'] = lat
    df_wind['longitude'] = lng
    df_wind.to_csv('C:/Users/benson/Desktop/IESO-ANA/IACG_wind.csv')

# get_solar_dataframe()
# get_wind_dataframe()




