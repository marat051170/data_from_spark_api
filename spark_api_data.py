import datetime
import math
import pandas as pd
from multiprocessing import Pool
from zeep import Client, Settings
from lists import dirs_dict
from save_results import save_to_excel
from save_results import save_to_postgre_db
from save_results import save_xml
from spark_xml_to_df import make_df
from spark_api_methods import methods_by_company_inn




NAME = 'xxxxx'
PASSWD = '******'
WSDL = 'http://webservicefarm.interfax.ru/IfaxWebService/ifaxwebservice.asmx?WSDL'
SETTINGS = Settings(strict = False, xml_huge_tree = True)
METHODS = methods_by_company_inn()
DIRS = dirs_dict()

poolsCount = 30 #1, 2, 4, 8, 16, 24, 30
COMPANIES = pd.read_excel(DIRS.get('SPARK_DATA_DIR') + 'список_компаний_спарк_120.xlsx', dtype = str)['ИНН'].unique()
setCount = math.ceil(len(COMPANIES) / poolsCount)
companies_sets = [COMPANIES[i:i + setCount] for i in range(0, len(COMPANIES), setCount)]
POOLS_NUMBER = 30 #if len(companies_sets) > 30 else len(companies_sets)



def get_data_by_method(client, method, company_id):
	resp = client.service[method]('', company_id, '')
	save_xml(str(resp['xmlData']), DIRS.get('SPARK_FILES'), 'test_spark_api_' + method + '_' + str(company_id) + '.xml')
	data = make_df(resp['xmlData'].encode(), 'Data', 'GetCompanyShortReport')
	save_to_excel(data, DIRS.get('SPARK_FILES'), 'test_spark_api_' + method + '_' + str(company_id) + '.xlsx')
	save_to_postgre_db(data, 'msp', 'spark_companies', list(data), 'Инфо о компании', 'append')
	return


def company_info(companies_set):
	client = Client(wsdl = WSDL, settings = SETTINGS)
	print('Сессия открыта ......................................................................... ', client.service.Authmethod(NAME, PASSWD))
	for company_id in companies_set:
		start = datetime.datetime.today()
		for method in METHODS:
			resp = get_data_by_method(client, method, company_id)
		end = datetime.datetime.today()
		work_info = {'потоки': POOLS_NUMBER, 'компания': str(company_id), 'старт': start, 'финиш': end, 'время обработки': end - start}
		work_info = pd.DataFrame(work_info, index = [0])
		save_to_excel(work_info, DIRS.get('SPARK_FILES'), 'test_spark_working_time_' + str(company_id) + '.xlsx')
	print('Cессия ЗАКРЫТА ......................................................................... ', client.service.End())
	return


if __name__ == '__main__':
	with Pool(POOLS_NUMBER) as p:
		p.map(company_info, companies_sets)
	