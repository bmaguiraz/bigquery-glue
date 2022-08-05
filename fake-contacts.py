import pandas as pd
import numpy as np
from faker import Faker
import random

# create some fake data
fake = Faker()

# function to create a dataframe with fake values for our workers
def make_workers(num):
    
    # lists to randomly assign to workers
    status_list = ['Full Time', 'Part Time', 'Per Diem']
    team_list = [fake.color_name() for x in range(4)]
    

    fake_workers = [{'Worker ID':x+1000,
                  'Worker Name':fake.name(), 
                  'Hire Date':fake.date_between(start_date='-30y', end_date='today'),
                  'Worker Status':np.random.choice(status_list, p=[0.50, 0.30, 0.20]), # assign items from list with different probabilities
                  'Team':np.random.choice(team_list)} for x in range(num)]
        
    return fake_workers

worker_df = pd.DataFrame(make_workers(num=5000))
worker_df.head()

# function to create widget data

def make_widget_data(num):
    
    fake_widgets = [{'Item Number':id(y),
                     'Step 1':np.random.gamma(shape=3, scale=1),
                     'Step 2':np.random.normal(5), 
                     'Step 3':np.random.exponential(4)} for y in range(num)]
    
    return fake_widgets

# empty list to store our widget dataframes in    
dfs_list = []

# now lets make some widget data for each worker
# iterate through the worker dataframe
for index, row in worker_df.iterrows():
    
    # not all workers work at the same rate - or the same number of hours
    # randomly select a number of widgets for them to create based on 'worker status'
    if row['Worker Status'] == 'Full Time':
        num_widgets = random.randrange(500, 1000)
    elif row['Worker Status'] == 'Part Time':
        num_widgets = random.randrange(100, 500)
    else:
        num_widgets = random.randrange(1, 1000)
    
    # make widgets for each worker
    tmp_widgets = pd.DataFrame(make_widget_data(num=num_widgets))
    
    # add worker id so we know who made the widget
    tmp_widgets['Worker ID'] = row['Worker ID']
    
    # make sure item number is unique by appending worker id
    tmp_widgets['Item Number'] = tmp_widgets['Item Number'].astype('str')+ '-' + tmp_widgets['Worker ID'].astype('str')
    
    # append to df list
    dfs_list.append(tmp_widgets)
    
# concatenate all the dfs 
widget_df = pd.concat(dfs_list)
print(widget_df.shape)
widget_df.head()
worker_df.to_csv('data/workers.csv', index=False)
widget_df.to_csv('data/widgets.csv', index=False)