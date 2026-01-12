from api_client import extract  
from transform import transform , load
from pandas import json_normalize


employee = 'https://raw.githubusercontent.com/Prane23/Python_ETL_Pipeline/refs/heads/master/data/employee.json'
employee_detail= 'https://raw.githubusercontent.com/Prane23/Python_ETL_Pipeline/refs/heads/master/data/employmentdetail.json'


try :
    emp_response= extract(employee)
    emp_detail_response= extract(employee_detail)
    #print(emp_response)
    #print(emp_detail_response)
    transformed_data= transform(json_normalize(emp_response,sep='_'),json_normalize(emp_detail_response,sep='_'))
    load(transformed_data)
except requests.HTTPError as http_error:
    print(f'Unable to call api, HTTP error occued {http_error}')
except Exception as exp:
    print(f'Unable to call api {exp}')
 
