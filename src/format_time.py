import sys
import pandas as pd

def time_formatter(input_csv):
    df = pd.read_csv(input_csv)
    # Parse the 'Time' column and reformat it
    df['Time'] = pd.to_datetime(df['Time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df.to_csv(input_csv, index=False)
    print('Formatting success')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("provide valid .csv file as argv 2")
        sys.exit(1)
    input_csv = sys.argv[1]
    time_formatter(input_csv)