import xlrd
import pandas as pd
import datetime

file_path='C:/Users/benson/Desktop/2016/GoC 2016.xlsx'


def minimalist_xldate_as_datetime(xldate, datemode):
    return (
        datetime.datetime(1899, 12, 30)
        + datetime.timedelta(days=xldate + 1462 * datemode)
        )

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

df=get_DataFrame_GeneratorOutputandCapability(file_path)
print df.shape
print df.head()