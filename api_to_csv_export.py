import requests as requests
import pandas as pd
from pandas import json_normalize

#extrac function to get data from api
def extract(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

#trasnform function to transform and merge data
def trabsform(emp, empdetail):
    try:
        emp_df= pd.DataFrame(emp)
        empdetail_df= pd.DataFrame(empdetail)
        merged_df= pd.merge(emp_df, empdetail_df, left_on='employee_employeeId',right_on='employmentdetail_employeeId',how='inner')
        #print(merged_df)
        final_df= merged_df[['employee_employeeId','employee_firstName','employee_lastName','employee_email',\
                             'employmentdetail_jobTitle', 'employmentdetail_department', 'employmentdetail_employmentType']]\
            .rename(columns={'employee_employeeId': 'EmployeeId','employee_firstName': 'FirstName',\
                            'employee_lastName': 'LastName','employee_email': 'Email','employmentdetail_jobTitle': 'JobTitle',\
                            'employmentdetail_department': 'Department','employmentdetail_employmentType': 'EmploymentType'})
    
        #print(final_df)
        return final_df
    except Exception as err :
        print(f'error: {err}')

#load function to export data to csv
def load(final_df):
    try:
        final_df.to_csv('employee_data.csv', index=False)
        print('Data exported to employee_data.csv successfully.')
    except Exception as err:
        print(f'error: {err}')

empployee = 'https://raw.githubusercontent.com/Prane23/Python_ETL_Pipeline/refs/heads/master/data/employee.json'
employee_detail= 'https://raw.githubusercontent.com/Prane23/Python_ETL_Pipeline/refs/heads/master/data/employmentdetail.json'

try :
    emp_response= extract(empployee)
    emp_detail_response= extract(employee_detail)
    #print(res)
    # print(emp_detail_response)
    trabsformed_data= trabsform(json_normalize(emp_response,sep='_'),json_normalize(emp_detail_response,sep='_'))
    load(trabsformed_data)
except requests.HTTPError as http_error:
    print(f'Unable to call api, HTTP error occued {http_error}')
except Exception as exp:
    print(f'Unable to call api {exp}')


 
