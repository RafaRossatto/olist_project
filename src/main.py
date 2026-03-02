from data_code.load_data import LoadData
from features.data_frame import DF

def main():
    # Exemplo 1: Uso básico
    path_in = '../data/raw/olist_orders_dataset.csv'
    path_out = "../data/data_process"
    
    try:
        loader = LoadData(path_in)
        df = loader.load() 
        df.save_data(path_out)
        #print(df.head())

    except FileNotFoundError:
        print("Deu erro...")
    
if __name__ == "__main__":
    main()