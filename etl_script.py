import os
import json
import pandas as pd
from sqlalchemy import create_engine

# --- CONFIGURATION ---
# UPDATE THIS PATH: It must point to the folder containing 'aggregated', 'map', 'top'
root_dir = "data/data" 

# ACTUAL CREDENTIALS
db_host = "localhost"
db_user = "root"
db_password = "#mySql123" 
db_name = "phonepe_pulse"

# Create SQLAlchemy Engine
# Note: Ensure you have mysql-connector-python and pymysql installed
engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")

def extract_aggregated_transaction():
    path = f"{root_dir}/aggregated/transaction/country/india/state/"
    data_list = []
    
    
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    
                    for item in data['data']['transactionData']:
                        row = {
                            "State": state.replace("-", " ").title(),
                            "Year": int(year),
                            "Quarter": int(file.replace(".json", "")),
                            "Transaction_Type": item['name'],
                            "Transaction_Count": item['paymentInstruments'][0]['count'],
                            "Transaction_Amount": item['paymentInstruments'][0]['amount']
                        }
                        data_list.append(row)
                except Exception as e:
                    pass 

    if data_list:
        df = pd.DataFrame(data_list)
        df.to_sql('aggregated_transaction', con=engine, if_exists='append', index=False)
        print(f"Aggregated Transaction: {len(df)} rows inserted.")

def extract_aggregated_user():
    path = f"{root_dir}/aggregated/user/country/india/state/"
    data_list = []


    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    
                    
                    users_data = data['data'].get('usersByDevice') 
                    if users_data:
                        for item in users_data:
                            row = {
                                "State": state.replace("-", " ").title(),
                                "Year": int(year),
                                "Quarter": int(file.replace(".json", "")),
                                "Brand": item['brand'],
                                "User_Count": item['count'],
                                "Percentage": item['percentage']
                            }
                            data_list.append(row)
                except Exception as e:
                    pass

    if data_list:
        df = pd.DataFrame(data_list)
        df.to_sql('aggregated_user', con=engine, if_exists='append', index=False)
        print(f"Aggregated User: {len(df)} rows inserted.")

def extract_aggregated_insurance():
    path = f"{root_dir}/aggregated/insurance/country/india/state/"
    data_list = []
    
    
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    
                    for item in data['data']['transactionData']:
                        row = {
                            "State": state.replace("-", " ").title(),
                            "Year": int(year),
                            "Quarter": int(file.replace(".json", "")),
                            "Transaction_Type": "Insurance",
                            "Transaction_Count": item['paymentInstruments'][0]['count'],
                            "Transaction_Amount": item['paymentInstruments'][0]['amount']
                        }
                        data_list.append(row)
                except Exception as e:
                    pass

    if data_list:
        df = pd.DataFrame(data_list)
        df.to_sql('aggregated_insurance', con=engine, if_exists='append', index=False)
        print(f"Aggregated Insurance: {len(df)} rows inserted.")

def extract_map_transaction():
    path = f"{root_dir}/map/transaction/hover/country/india/state/"
    data_list = []
    
   
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        
                    for item in data['data']['hoverDataList']:
                        row = {
                            "State": state.replace("-", " ").title(),
                            "Year": int(year),
                            "Quarter": int(file.replace(".json", "")),
                            "District": item['name'],
                            "Transaction_Count": item['metric'][0]['count'],
                            "Transaction_Amount": item['metric'][0]['amount']
                        }
                        data_list.append(row)
                except Exception as e:
                    pass

    if data_list:
        df = pd.DataFrame(data_list)
        df.to_sql('map_transaction', con=engine, if_exists='append', index=False)
        print(f"Map Transaction: {len(df)} rows inserted.")

def extract_map_user():
    path = f"{root_dir}/map/user/hover/country/india/state/"
    data_list = []
    
    
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        
                   
                    hover_data = data.get('data', {}).get('hoverData')
                    if hover_data:
                        for district, metrics in hover_data.items():
                            row = {
                                "State": state.replace("-", " ").title(),
                                "Year": int(year),
                                "Quarter": int(file.replace(".json", "")),
                                "District": district,
                                "Registered_Users": metrics['registeredUsers'],
                                "App_Opens": metrics['appOpens']
                            }
                            data_list.append(row)
                except Exception as e:
                    pass

    if data_list:
        df = pd.DataFrame(data_list)
        df.to_sql('map_user', con=engine, if_exists='append', index=False)
        print(f"Map User: {len(df)} rows inserted.")

def extract_map_insurance():
    path = f"{root_dir}/map/insurance/hover/country/india/state/"
    data_list = []
    

    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        
                    for item in data['data']['hoverDataList']:
                        row = {
                            "State": state.replace("-", " ").title(),
                            "Year": int(year),
                            "Quarter": int(file.replace(".json", "")),
                            "District": item['name'],
                            "Insurance_Count": item['metric'][0]['count'],
                            "Insurance_Amount": item['metric'][0]['amount']
                        }
                        data_list.append(row)
                except Exception as e:
                    pass

    if data_list:
        df = pd.DataFrame(data_list)
        df.to_sql('map_insurance', con=engine, if_exists='append', index=False)
        print(f"Map Insurance: {len(df)} rows inserted.")

def extract_top_transaction():
    path = f"{root_dir}/top/transaction/country/india/state/"
    data_list = []
    

    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    
                    # 1. Extract Districts
                    for item in data['data']['districts']:
                        row = {
                            "State": state.replace("-", " ").title(),
                            "Year": int(year),
                            "Quarter": int(file.replace(".json", "")),
                            "Entity_Name": item['entityName'],
                            "Metric_Type": "District",
                            "Transaction_Count": item['metric']['count'],
                            "Transaction_Amount": item['metric']['amount']
                        }
                        data_list.append(row)
                    
                    # 2. Extract Pincodes
                    for item in data['data']['pincodes']:
                        row = {
                            "State": state.replace("-", " ").title(),
                            "Year": int(year),
                            "Quarter": int(file.replace(".json", "")),
                            "Entity_Name": str(item['entityName']),
                            "Metric_Type": "Pincode",
                            "Transaction_Count": item['metric']['count'],
                            "Transaction_Amount": item['metric']['amount']
                        }
                        data_list.append(row)

                except Exception as e:
                    pass

    if data_list:
        df = pd.DataFrame(data_list)
        df.to_sql('top_transaction', con=engine, if_exists='append', index=False)
        print(f"Top Transaction: {len(df)} rows inserted.")

def extract_top_user():
    path = f"{root_dir}/top/user/country/india/state/"
    data_list = []
    
   
    for state in os.listdir(path):
        state_path = os.path.join(path, state)       
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    
                    
                    for item in data['data']['districts']:
                        row = {
                            "State": state.replace("-", " ").title(),
                            "Year": int(year),
                            "Quarter": int(file.replace(".json", "")),
                            "Entity_Name": item['name'], # Different Key
                            "Metric_Type": "District",
                            "Registered_Users": item['registeredUsers'] # Different Key
                        }
                        data_list.append(row)

                    for item in data['data']['pincodes']:
                        row = {
                            "State": state.replace("-", " ").title(),
                            "Year": int(year),
                            "Quarter": int(file.replace(".json", "")),
                            "Entity_Name": str(item['name']), # Different Key
                            "Metric_Type": "Pincode",
                            "Registered_Users": item['registeredUsers'] # Different Key
                        }
                        data_list.append(row)
                except Exception as e:
                    pass

    if data_list:
        df = pd.DataFrame(data_list)
        df.to_sql('top_user', con=engine, if_exists='append', index=False)
        print(f"Top User: {len(df)} rows inserted.")

def extract_top_insurance():
    path = f"{root_dir}/top/insurance/country/india/state/"
    data_list = []
    
    
    for state in os.listdir(path):
        state_path = os.path.join(path, state)
        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            for file in os.listdir(year_path):
                file_path = os.path.join(year_path, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    
                    for item in data['data']['districts']:
                        row = {
                            "State": state.replace("-", " ").title(),
                            "Year": int(year),
                            "Quarter": int(file.replace(".json", "")),
                            "Entity_Name": item['entityName'],
                            "Metric_Type": "District",
                            "Insurance_Count": item['metric']['count'],
                            "Insurance_Amount": item['metric']['amount']
                        }
                        data_list.append(row)

                    for item in data['data']['pincodes']:
                        row = {
                            "State": state.replace("-", " ").title(),
                            "Year": int(year),
                            "Quarter": int(file.replace(".json", "")),
                            "Entity_Name": str(item['entityName']),
                            "Metric_Type": "Pincode",
                            "Insurance_Count": item['metric']['count'],
                            "Insurance_Amount": item['metric']['amount']
                        }
                        data_list.append(row)
                except Exception as e:
                    pass

    if data_list:
        df = pd.DataFrame(data_list)
        df.to_sql('top_insurance', con=engine, if_exists='append', index=False)
        print(f"Top Insurance: {len(df)} rows inserted.")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print("Starting ETL Process...")

    extract_aggregated_transaction()
    extract_aggregated_user()
    extract_aggregated_insurance()
    
    extract_map_transaction()
    extract_map_user()
    extract_map_insurance()
    
    extract_top_transaction()
    extract_top_user()
    extract_top_insurance()
    
    print("ETL Process Completed!")