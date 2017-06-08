import xlrd
import pandas as pd
import datetime

file_path='C:/Users/benson/Desktop/2016-example/RealtimeMarketPriceReport_2016.xlsx'

def minimalist_xldate_as_datetime(xldate, datemode):
    return (
        datetime.datetime(1899, 12, 30)
        + datetime.timedelta(days=xldate + 1462 * datemode)
        )

def get_DataFrame_RealtimeMarketPrice(filePath):
    wb = xlrd.open_workbook(file_path)
    output_sheet = wb.sheet_by_name(u'RealtimeMarketPriceReport_2016_')
    first_row_output = output_sheet.row_values(0)
    first_col_output=output_sheet.col_values(0)
    data_list = []
    dict = {}

    for i in range(2,len(first_col_output),1):
        output_date=output_sheet.cell(i, 0).value
        o_date=datetime.datetime.strptime(output_date,'%m/%d/%Y')
        output_hour=output_sheet.cell(i, 1).value
        output_minute = output_sheet.cell(i, 2).value
        dict['datetime']=o_date\
                         +datetime.timedelta(hours=(int(output_hour)-1))\
                         +datetime.timedelta(minutes=(int(output_minute)-1)*5)
        for j in range(3,len(first_row_output),1):
            str1=output_sheet.cell(0, j).value
            str2=output_sheet.cell(1, j).value
            str='%s_%s'%(str1,str2)
            output_value = output_sheet.cell(i, j).value
            dict[str] =output_value
            # dict['output']=output_value
            # capabilities_value = capabilities_sheet.cell(i, j-1).value
            # dict['capabilities'] = capabilities_value
            # available_capacities_value = available_capacities_sheet.cell(i, j-1).value
            # dict['available_capacities'] = available_capacities_value
        dict2 = {}
        dict2.update(dict)
        data_list.append(dict2)
    return pd.DataFrame.from_dict(data_list)

df=get_DataFrame_RealtimeMarketPrice(file_path)
df.to_csv('C:/Users/benson/Desktop/2016-example/RealtimeMarketPriceReport_2016.csv', header=True)
print df.shape
print df.head()