import argparse

from data_code.load_data import LoadData
from features.data_frame import DF

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True, help='Arquivo de entrada')
    parser.add_argument('-o', '--output', required=True, help='Diretório de saída')
    parser.add_argument('-m', '--metric', required=True, help='Metric: time_to_approved,transit_time,comparation_time,total_time, all')
    
    args = parser.parse_args()
    
    #path_in = '../data/raw/olist_orders_dataset.csv'
    #path_out = "../data/data_process"

        # Valida se o tipo é válidols
        
    metrics = ['time_to_approved', 'transit_time', 'comparation_time', 'total_time', 'all']
    if args.metric not in metrics:
        print(f" Error: Metric '{args.metric}' invalid!")
        print(f" Valid Metric: {', '.join(metrics)}")
        return
    
    try:
        loader = LoadData(args.input)
        df = loader.load() 
        df.save_data(args.output,args.metric)

    except FileNotFoundError:
        print("File not found, please verify the file path.")
    
if __name__ == "__main__":
    main()