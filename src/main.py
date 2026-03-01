from data.load_data import LoadData

def main():
    # Exemplo 1: Uso básico
    print("=== EXEMPLO 1: Básico ===")
    path = LoadData('../data/raw/olist_orders_dataset.csv')
    
    try:
        df = path.load()
        print(f"DataFrame carregado: {df.shape}")
        print(df.head())
        
    except FileNotFoundError:
        print("Deu erro...")
        # Cria dados de exemplo
    
if __name__ == "__main__":
    main()