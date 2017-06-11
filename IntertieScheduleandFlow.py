import xlrd
import pandas as pd
import datetime

file_path='C:/Users/benson/Desktop/2015-csv/Intertie Schedule and Flow Reports/ISF 2016.xlsx'

def minimalist_xldate_as_datetime(xldate, datemode):
    return (
        datetime.datetime(1899, 12, 30)
        + datetime.timedelta(days=xldate + 1462 * datemode)
        )


def get_DataFrame_RealtimeMarketPrice(filePath):
    wb = xlrd.open_workbook(file_path)
    schedule = wb.sheet_by_name(u'Schedules')
    flow = wb.sheet_by_name(u'Flows')
    first_row_schedule = schedule.row_values(0)
    first_col_schedule=schedule.col_values(0)
    first_row_flow = flow.row_values(0)
    data_list = []
    dict = {}
    flow_row_length=len(first_row_flow)
    for i in range(2,len(first_col_schedule)-2,1):
        output_date=schedule.cell(i, 0).value
        o_date=minimalist_xldate_as_datetime(output_date,0)
        output_hour=schedule.cell(i, 1).value
        dict['datetime']=o_date+datetime.timedelta(hours=(int(output_hour)-1))
        print dict['datetime']
        for j in range(3,len(first_row_schedule),1):
            str1=schedule.cell(0, j).value
            str2=schedule.cell(1, j).value
            str='%s_%s'%(str1,str2)
            output_value = schedule.cell(i, j).value
            dict[str] =output_value
        for k in range(3,flow_row_length,1):
            str3 = flow.cell(0, k).value
            str4 = flow.cell(1, k).value
            str_flow = '%s_%s' % (str3, str4)
            # print str
            output_value = flow.cell(i, k).value
            dict[str_flow] = output_value


        dict2 = {}
        dict2.update(dict)
        data_list.append(dict)
        dict={}

    return pd.DataFrame.from_dict(data_list)

df=get_DataFrame_RealtimeMarketPrice(file_path)
df.to_csv('C:/Users/benson/Desktop/2015-csv/Intertie Schedule and Flow Reports/ISF2016_day.csv')
print df.shape
print df.head()