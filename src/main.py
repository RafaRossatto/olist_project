from data_code.load_data import LoadData
from features.data_frame import DF

def main():
    # Exemplo 1: Uso básico
    print("=== EXEMPLO 1: Básico ===")
    path = '../data/raw/olist_orders_dataset.csv'
    
    try:
        loader = LoadData(path)
        df = loader.load() 
        df.summary()
        print(df.head())

    except FileNotFoundError:
        print("Deu erro...")
    
if __name__ == "__main__":
    main()