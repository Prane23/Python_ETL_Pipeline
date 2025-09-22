import requests as requests
import pandas as pd
from pandas import json_normalize

# call api
def extract(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def trabsform(emp, empdetail):
    try:
        emp_df= pd.DataFrame(emp)
        empdetail_df= pd.DataFrame(empdetail)
        merged_df= pd.merge(emp_df, empdetail_df, left_on='employee_employeeId',right_on='employmentdetail_employeeId',how='inner')
        #print(merged_df)
        final_df= merged_df[['employee_employeeId','employee_firstName','employee_lastName']].\
            rename(columns={'employee_employeeId': 'EmployeeId','employee_firstName': 'FirstName','employee_lastName': 'LastName'})

        print(final_df)
    except Exception as err :
        print(f'error: {err}')



empployee = 'https://raw.githubusercontent.com/Prane23/Python_ETL_Pipeline/refs/heads/master/data/employee.json'
employee_detail= 'https://raw.githubusercontent.com/Prane23/Python_ETL_Pipeline/refs/heads/master/data/employmentdetail.json'

try :
    emp_response= extract(empployee)
    emp_detail_response= extract(employee_detail)
    #print(res)
    # print(emp_detail_response)
    trabsform(json_normalize(emp_response,sep='_'),json_normalize(emp_detail_response,sep='_'))
   
except requests.HTTPError as http_error:
    print(f'Unable to call api, HTTP error occued {http_error}')
except Exception as exp:
    print(f'Unable to call api {exp}')


 
