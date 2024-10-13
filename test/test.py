from deltalake import DeltaTable
import time
import os

# Polling to simulate streaming (continuously read new data)
def read_delta_table(delta_table_path):
    while True:
        # Instantiate your delta table
        delta_table = DeltaTable(delta_table_path)
        # Get the latest version of the Delta Table
        version = delta_table.version()
        print(f"Reading data from version {version}")
        # Load the table as a pandas dataframe
        df = delta_table.to_pandas()
        print('shape of delta_table',df.shape)
        # Wait for a few seconds before polling again
        time.sleep(100)

if __name__ == "__main__":
    # Path to your Delta Lake table
    delta_table_path = os.getenv('DELTA_SINK_PATH')
    read_delta_table(delta_table_path)