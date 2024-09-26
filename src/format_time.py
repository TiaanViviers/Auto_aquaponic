import sys
import pandas as pd

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py input.csv")
        sys.exit(1)

    input_csv = sys.argv[1]

    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_csv)

    # Parse the 'Time' column and reformat it
    df['Time'] = pd.to_datetime(df['Time']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Write back to the same CSV file
    df.to_csv(input_csv, index=False)

if __name__ == '__main__':
    main()