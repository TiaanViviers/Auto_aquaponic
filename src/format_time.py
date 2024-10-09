import sys
import pandas as pd

def time_formatter(input_csv):
    """
    Formats the 'Time' column of the input CSV to a standard format (YYYY-MM-DD HH:MM:SS).
    
    Args:
        input_csv (str): The path to the input CSV file containing the 'Time' column.
        
    Functionality:
        - Reads the input CSV file.
        - Converts and formats the 'Time' column to the format 'YYYY-MM-DD HH:MM:SS'.
        - Overwrites the original CSV file with the reformatted time values.
        - Prints a success message upon completion.
    
    Raises:
        FileNotFoundError: If the specified CSV file does not exist.
        KeyError: If the 'Time' column is missing from the CSV file.
    """
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