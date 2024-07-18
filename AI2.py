import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from scipy.stats import zscore


host = 'localhost'
username = 'root'
password = '123'
database = 'IoTLocal3'

try:
    
    connection = mysql.connector.connect(
        host=host,
        user=username,
        password=password,
        database=database
    )

    if connection.is_connected():
        
        query = """
        SELECT created_date, value_list_id, data_value, device_serial_no
        FROM hourly_data
        ORDER BY created_date
        """

        df = pd.read_sql(query, con=connection)

        connection.close()

        # relevant_value_list_ids = [702,703,704]
        relevant_value_list_ids = [802,803,804]

        df = df[df['value_list_id'].isin(relevant_value_list_ids)]


        scaler = StandardScaler()
        df['data_value_normalized'] = scaler.fit_transform(df['data_value'].values.reshape(-1, 1))


        pca = PCA(n_components=1)
        df['principal_component'] = pca.fit_transform(df['data_value_normalized'].values.reshape(-1, 1))


        df['z_score'] = zscore(df['principal_component'])

        threshold = 3.6  #******************************************************************************************
        df['anomaly_score'] = np.abs(df['z_score']) > threshold

        unique_devices = df['device_serial_no'].unique()
        for device in unique_devices:
            for value_list_id in relevant_value_list_ids:
                device_data = df[(df['device_serial_no'] == device) & (df['value_list_id'] == value_list_id)]
                anomalies = device_data[device_data['anomaly_score']]
                
                plt.figure(figsize=(9, 6))
                plt.plot(device_data['created_date'], device_data['data_value'], label='Data')
                
                if not anomalies.empty:
                    plt.scatter(anomalies['created_date'], anomalies['data_value'], color='red', label='Anomalies (RPCA)')
                
                plt.title(f'Anomaly Detection using RPCA for PIER - Device {device}, Value List ID {value_list_id}')
                plt.xlabel('Date')
                plt.ylabel('Data Value')
                plt.legend()
                plt.grid(True)
                

                filename = f'anomaly_device_{device}_value_list_id_{value_list_id}.png'
                plt.savefig(filename)
                plt.close()
                print(f'Saved plot for device {device}, value_list_id {value_list_id} as {filename}')

                if not anomalies.empty:
                    print(f"\nDetected anomalies for device {device}, value_list_id {value_list_id}:" + "\u2715")
                    print(anomalies[['created_date', 'data_value']])

    else:
        print('Connection to MySQL database failed')

except mysql.connector.Error as error:
    print(f'Error: {error}')
