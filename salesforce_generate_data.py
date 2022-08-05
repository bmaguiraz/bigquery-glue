import os
import csv
import pandas as pd 
from snowfakery import generate_data

generate_data("salesforce/Account.recipe.yml",target_number=(100,"Account"),output_folder="data/salesforce",output_format="csv" )

generate_data("salesforce/ContactsForAccountsFull.recipe.yml",target_number=(100,"Contact"),output_folder="data/salesforce",output_format="csv")


# cleanup temp file

  # cleanup temp file
if os.path.exists("data/salesforce/csvw_metadata.json"):
  os.remove("data/salesforce/csvw_metadata.json")
else:
  print("The file does not exist")



# clean up the CSV files

csv_file='data/salesforce/Account.csv'

csv_table=pd.read_csv(csv_file,delimiter=None)
csv_table.to_csv('data/salesforce/Account.csv',index=False,header=True, quoting=csv.QUOTE_ALL)

csv_file='data/salesforce/Contact.csv'
  
csv_table=pd.read_csv(csv_file,delimiter=None)
csv_table.to_csv('data/salesforce/Contact.csv',index=False,header=True, quoting=csv.QUOTE_ALL)
  


