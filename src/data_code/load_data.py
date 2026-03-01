from features.data_frame import DF

class LoadData:
    def __init__(self, path: str):
        self.path = path
    
    def load(self, **kwargs) -> DF:
        """
        Carrega dados usando métodos da própria classe DF.
        """
        if self.path.endswith('.csv'):
            df = DF.read_csv(self.path, **kwargs)
        elif self.path.endswith('.xlsx'):
            df = DF.read_excel(self.path, **kwargs)
        elif self.path.endswith('.json'):
            df = DF.read_json(self.path, **kwargs)
        else:
            raise ValueError(f"Formato não suportado: {self.path}")
        
        return df 