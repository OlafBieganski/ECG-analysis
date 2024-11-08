import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'MySQL123',
    'database': 'ecg_database'
}

# Connect to MySQL database
def connect_to_database():
    try:
        conn = mysql.connector.connect(**db_config)
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        exit(1)

# Upload data to MySQL
def upload_data_to_mysql(conn, file_path, table_name):
    cursor = conn.cursor()

    # Read CSV data, use 3rd column as 'sample' and 7th as 'value'
    df = pd.read_csv(file_path, header=None, usecols=[2, 6], names=['sample', 'value'])

    # Extract time (hour, minute, second, millisecond) from 'sample' column
    df['sample'] = pd.to_datetime(df['sample']).dt.strftime('%H:%M:%S.%f')

    print(f"Uploading {len(df)} rows from {file_path} to table {table_name}")
    print(df.head())  # Debugging: Print first few rows

    # Create table if it doesn't exist
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sample VARCHAR(255),
            value FLOAT
        );
    """)

    # Prepare data for executemany (list of tuples)
    data_to_insert = list(df.itertuples(index=False, name=None))

    # Use executemany for batch insert
    cursor.executemany(f"INSERT INTO {table_name} (sample, value) VALUES (%s, %s)", data_to_insert)

    conn.commit()
    cursor.close()

# Read data from MySQL
def read_data_from_mysql(conn, table_name):
    cursor = conn.cursor()
    query = f"SELECT sample, value FROM {table_name};"
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result, columns=['sample', 'value'])

    # Convert 'sample' to datetime object
    df['sample'] = pd.to_datetime(df['sample'], format='%H:%M:%S.%f')

    return df

# Plot the ECG data
def plot_data(df, title):
    plt.figure(figsize=(10, 6))
    plt.plot(df['sample'], df['value'], label=title)

    plt.title(title)
    plt.xlabel('Time (HH:MM:SS.ms)')
    plt.ylabel('Value')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()

# Apply and plot Fourier Transform (FFT) of the data
def plot_fourier_transform(df, title):
    # Detrend the data by removing the mean (center it at zero)
    values = df['value'].values
    values = values - np.mean(values)  # Remove the DC component (mean)

    # Compute the Fourier Transform
    fft_values = np.fft.fft(values)
    freqs = np.fft.fftfreq(len(values))

    # Plot the FFT
    plt.figure(figsize=(10, 6))
    plt.plot(freqs, np.abs(fft_values), label=f"FFT of {title}")

    plt.title(f"Fourier Transform - {title}")
    plt.xlabel('Frequency')
    plt.ylabel('Amplitude')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()


# Main function to handle the workflow
def main():
    # File paths (correct path format)
    path = r"C:\Users\olafb\OneDrive\Pulpit\Magisterka\IKA semestr 2\Eksploracja danych laby\ECG data"
    
    file_names = [
        'AD8232/ECG_samples_AD_arm_no_move.csv',
        'AD8232/ECG_samples_AD_chest_in_move.csv',
        'AD8232/ECG_samples_AD_chest_no_move.csv',
        'AD8232/ECG_samples_AD_arm_in_move.csv',
        'MAX30003/ECG_samples_MAX_arm_in_move.csv',
        'MAX30003/ECG_samples_MAX_arm_no_move.csv',
        'MAX30003/ECG_samples_MAX_chest_in_move.csv',
        'MAX30003/ECG_samples_MAX_chest_no_move.csv'
    ]

    # Table names
    table_names = [
        'ecg_arm_no_move',
        'ecg_chest_in_move',
        'ecg_chest_no_move',
        'ecg_arm_in_move',
        'ecg_max_arm_in_move',
        'ecg_max_arm_no_move',
        'ecg_max_chest_in_move',
        'ecg_max_chest_no_move'
    ]

    # Connect to the database
    conn = connect_to_database()

    # Ask the user if they want to upload all data
    upload_decision = input("Do you want to upload all data? (y/n): ").strip().lower()

    # Iterate over each file and insert into the corresponding table
    for i, file_name in enumerate(file_names):
        full_path = os.path.join(path, file_name)
        print(full_path)

        if upload_decision == 'y':
            try:
                # Upload data from CSV files to MySQL
                upload_data_to_mysql(conn, full_path, table_names[i])
                print(f"Data from {file_name} uploaded to {table_names[i]} table.")
            except Exception as e:
                print(f"Error while uploading {file_name} to {table_names[i]}: {e}")
        else:
            try:
                # Read data back from the database and plot it
                df = read_data_from_mysql(conn, table_names[i])
                if not df.empty:
                    plot_data(df, title=f"ECG Data - {table_names[i]}")
                    plot_fourier_transform(df, title=f"ECG Data - {table_names[i]} Fourier Transform")
                else:
                    print(f"No data available in table {table_names[i]} to plot.")
            except Exception as e:
                print(f"Error reading from {table_names[i]}: {e}")

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()
