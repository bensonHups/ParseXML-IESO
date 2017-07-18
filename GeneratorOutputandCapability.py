import xlrd
import pandas as pd
import datetime

file_path='C:/Users/benson/Desktop/2016-example/GoC 2016.xlsx'

def minimalist_xldate_as_datetime(xldate, datemode):
    return (
        datetime.datetime(1899, 12, 30)
        + datetime.timedelta(days=xldate + 1462 * datemode)
        )

def hour_dif(t1,t2):
    t=t1-t2
    hour_t=t.days*24+(t.seconds)/(60*60)
    return hour_t
def get_DataFrame_GeneratorOutputandCapability(filePath):
    wb = xlrd.open_workbook(file_path)
    sheet_names=wb.sheet_names()

    output_sheet = wb.sheet_by_name(u'Output')
    capabilities_sheet = wb.sheet_by_name(u'Capabilities')
    available_capacities_sheet=wb.sheet_by_name(u'Available Capacities')
    first_row_output = output_sheet.row_values(0)
    first_col_output=output_sheet.col_values(0)
    data_list = []
    dict = {}

    for i in range(1,len(first_col_output),1):
        output_date=output_sheet.cell(i, 0).value
        output_hour=output_sheet.cell(i, 1).value
        dict['datetime']=minimalist_xldate_as_datetime(output_date, 0)+datetime.timedelta(hours=(int(output_hour)-1))

        for j in range(3,len(first_row_output),1):
            output_title=output_sheet.cell(0, j).value
            dict['title']=output_title
            output_value = output_sheet.cell(i, j).value
            dict['output']=output_value
            capabilities_value = capabilities_sheet.cell(i, j-1).value
            dict['capabilities'] = capabilities_value
            available_capacities_value = available_capacities_sheet.cell(i, j-1).value
            dict['available_capacities'] = available_capacities_value
            dict2 = {}
            dict2.update(dict)
            data_list.append(dict2)
    for i in range(1, len(first_col_output), 1):
        output_date = output_sheet.cell(i, 0).value
        output_hour = output_sheet.cell(i, 1).value
        dict['datetime'] = minimalist_xldate_as_datetime(output_date, 0) + datetime.timedelta(
            hours=(int(output_hour) - 1))
        dict['title'] = 'total'
        output_value = output_sheet.cell(i, 2).value
        dict['output'] = output_value
        dict['capabilities']=-1
        dict['available_capacities']=-1
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)

    return pd.DataFrame.from_dict(data_list)

def generate_GeneratorOutputandCapability_Table(startdate,enddate,header):
    day_hourlist=pd.date_range('%s 00:00:00'%startdate,'%s 23:00:00'%enddate,freq='H')
    dict = {}
    data_list = []
    for i in range(len(day_hourlist)):
        dict['datetime']=day_hourlist[i]
        for j in range(len(header)):
            str=header[j]
            dict[str]=None
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
csv_file='C:/Users/benson/Desktop/2016-example/GoC2016.csv'
def year_csv2day_GeneratorOutputandCapability():
    df = pd.read_csv(csv_file)
    headers = df.groupby(df['title'])
    head_list = []
    for head in headers:
        head_list.append(head[0])
    head_cha=['available_capacities','capabilities','output']
    headers=[]
    for i in range(len(head_list)):
        for j in range(len(head_cha)):
            headers.append('GeneratorOutputandCapability_%s_%s'%(head_list[i],head_cha[j]))
    df_save = generate_GeneratorOutputandCapability_Table('2016-01-01','2016-12-31',headers)
    for index in df.index:
        str_title = df.loc[index, ['title']][0]
        dtime = df.loc[index, ['datetime']][0]
        dtime_y = datetime.datetime.strptime(dtime, '%Y-%m-%d %H:%M:%S')
        str_capacities = 'GeneratorOutputandCapability_%s_available_capacities' % str_title
        available_capacities = df.loc[index, ['available_capacities']][0]
        df_save.set_value(dtime_y,str_capacities,available_capacities,takeable=False)


        str_capabilities = 'GeneratorOutputandCapability_%s_capabilities' % str_title
        capabilities = df.loc[index, ['capabilities']][0]
        df_save.set_value(dtime_y, str_capabilities, capabilities, takeable=False)

        str_output = 'GeneratorOutputandCapability_%s_output' % str_title
        output = df.loc[index, ['output']][0]
        df_save.set_value(dtime_y, str_output, output, takeable=False)

    df_save.to_csv('C:/Users/benson/Desktop/2016-example/GoC2016_day.csv' )

year_csv2day_GeneratorOutputandCapability()

