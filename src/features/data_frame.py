import pandas as pd

class DF(pd.DataFrame):
    """
    Classe personalizada que herda de pandas.DataFrame
    """
    
    # Propriedade para identificar nossa classe
    #_metadata = ['nome']
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
       # self.nome = kwargs.get('nome', 'DataFrame sem nome')
    
    # @property
    # def _constructor(self):
    #     return DF
    
    @classmethod
    def read_csv(cls, path, **kwargs):
        """
        Lê um arquivo CSV e retorna como DF personalizado.
        
        Args:
            path: Caminho do arquivo CSV
            **kwargs: Argumentos adicionais do pd.read_csv
                      (encoding, sep, header, etc)
        
        Returns:
            DF: DataFrame personalizado
        """
        # 1. Lê o CSV usando pandas
        df_pandas = pd.read_csv(path, **kwargs)
        
        # 2. Converte para DF personalizado e retorna
        return cls(df_pandas)

    @classmethod
    def read_exel(cls, path, **kwargs):
        """
        Lê um arquivo Excel e retorna como DF personalizado.
    
        Args:
            path: Caminho do arquivo Excel
            **kwargs: Argumentos adicionais do pd.read_excel
                    - sheet_name: Nome ou índice da planilha (default: 0)
                    - header: Linha do cabeçalho (default: 0)
                    - skiprows: Linhas a pular
                    - nrows: Número de linhas a ler
        
        Returns:
            DF: DataFrame personalizado
        
        Exemplos:
            df = DF.read_excel('dados.xlsx')
            df = DF.read_excel('dados.xlsx', sheet_name='Vendas')
            df = DF.read_excel('dados.xlsx', sheet_name=0, skiprows=2)
        """
        # 1. Lê o CSV usando pandas
        df_pandas = pd.read_excel(path, **kwargs)
        
        # 2. Converte para DF personalizado e retorna
        return cls(df_pandas)

    @classmethod
    def read_json(cls, path, **kwargs):
        """
        Lê um arquivo JSON e retorna como DF personalizado.
        
        Args:
            path: Caminho do arquivo JSON
            **kwargs: Argumentos adicionais do pd.read_json
                    (orient, encoding, lines, etc)
        
        Returns:
            DF: DataFrame personalizado
        
        Exemplos:
            df = DF.read_json('dados.json')
            df = DF.read_json('dados.json', orient='records')
            df = DF.read_json('dados.json', encoding='utf-8')
        """
        # 1. Lê o JSON usando pandas
        df_pandas = pd.read_json(path, **kwargs)
        
        # 2. Converte para DF personalizado e retorna
        return cls(df_pandas)



    
    # Seus métodos personalizados
    def summary(self):
        """Resumo estatístico personalizado"""
       # print(f"\n📊 Resumo do DataFrame: {self.nome}")
        print(f"📏 Dimensões: {self.shape}")
        print(f"🔤 Colunas: {list(self.columns)}")
        print(f"\n📈 Estatísticas:")
        return self.describe()
    
    def drop_na_cols(self, threshold=0.5):
        """
        Remove colunas com mais de threshold% de valores nulos
        """
        null_pct = self.isnull().sum() / len(self)
        cols_to_drop = null_pct[null_pct > threshold].index
        return self.drop(columns=cols_to_drop)