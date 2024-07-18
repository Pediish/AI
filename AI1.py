import mysql.connector
import pandas as pd
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt

host = 'localhost'
username = 'root'
password = '123'
database = 'IoTLocal2'
try:
    connection = mysql.connector.connect(
        host=host,
        user=username,
        password=password,
        database=database
    )

    if connection.is_connected():
        # print('Connected to MySQL database')

        query = """
        SELECT created_date, value_list_id, data_value, device_serial_no
        FROM hourly_data
        ORDER BY created_date
        """

        df = pd.read_sql(query, con=connection)

        connection.close()
        # print('MySQL connection closed')

        df['created_date'] = pd.to_datetime(df['created_date'])

        relevant_value_list_ids = [802, 803, 804, 702, 703, 704]
        df = df[df['value_list_id'].isin(relevant_value_list_ids)]

        def plot_clusters(df, value_list_id, eps, min_samples, device_serial_no, save_path=None):
            data = df[(df['value_list_id'] == value_list_id) & (df['device_serial_no'] == device_serial_no)][['created_date', 'data_value']]

            X = data['data_value'].values.reshape(-1, 1)

            model = DBSCAN(eps=eps, min_samples=min_samples)
            model.fit(X)
            labels = model.labels_

            anomalies_mask = labels == -1
            anomalies = data.loc[anomalies_mask]

            plt.figure(figsize=(10, 6))

            unique_labels = set(labels)
            for label in unique_labels:
                if label == -1:
                    plt.scatter(data.loc[labels == label, 'created_date'], data.loc[labels == label, 'data_value'], 
                                c='red', marker='x', s=100, label='Anomalies (Noise Points)')
                else:
                    plt.scatter(data.loc[labels == label, 'created_date'], data.loc[labels == label, 'data_value'], 
                                label=f'Cluster {label}')

            plt.title(f'DBSCAN Clustering for value_list_id {value_list_id}, Device {device_serial_no}')
            plt.xlabel('Date')
            plt.ylabel('Data Value')
            plt.legend()
            plt.grid(True)

            if save_path:
                plt.savefig(save_path)
                # print(f"Saved plot as {save_path}")
                plt.close()
            else:
                plt.show()

            if not anomalies.empty:
                print(f"\nAbnormal data points for value_list_id {value_list_id}, Device {device_serial_no}: " + "\u2715" )
                print(anomalies[['created_date', 'data_value']].reset_index(drop=True))
                print()

        for value_list_id in relevant_value_list_ids:
            eps, min_samples = 9.9 , 3
            
            
            unique_devices = df['device_serial_no'].unique()
            
            for device_serial_no in unique_devices:
                plot_clusters(df, value_list_id, eps, min_samples, device_serial_no, save_path=f'value_list_id_{value_list_id}_device_{device_serial_no}_dbscan_plot.png')

    else:
        print('Connection to MySQL database failed')

except mysql.connector.Error as error:
    print(f'Error: {error}')
