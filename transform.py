import pandas as pd

#transform function to transform and merge data
def transform(emp, empdetail):
    try:
        emp_df= pd.DataFrame(emp)
        empdetail_df= pd.DataFrame(empdetail)
        #print(empdetail_df)
        merged_df= pd.merge(emp_df, empdetail_df, on='employeeId',how='inner')
        #print(merged_df)
        final_df= merged_df[['employeeId','firstName','lastName','email',\
                             'jobTitle', 'department', 'employmentType','status','skipManager','hireDate']]\
            .rename(columns={'employeeId': 'EmployeeId','firstName': 'FirstName',\
                            'lastName': 'LastName','email': 'Email','jobTitle': 'JobTitle',\
                            'department': 'Department','employmentType': 'EmploymentType',\
                            'status': 'Status','skipManager': 'SkipManager','hireDate': 'HireDate'})
    
        #print(final_df)
        return final_df
    except Exception as exp :
        print(f'Error occured while transforming data: {exp}')


#load function to export data to csv
def load(final_df):
    try:
        final_df.to_csv('employee_data.csv', index=False)
        print('Data exported to employee_data.csv successfully.')
    except Exception as exp:
        print(f'Error occured while loading data: {exp}')