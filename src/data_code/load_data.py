import pandas as pd
import os

class LoadData:
    """
    Classe para carregar dados de arquivos CSV.
    
    Exemplos de uso:
        loader = LoadData('dados.csv')
        df = loader.carregar()
        
        loader = LoadData('dados.csv', encoding='latin1', sep=';')
        df = loader.carregar()
    """
    
    def __init__(self, path: str, encoding: str = 'utf-8', sep: str = ','):
        """
        Inicializa o carregador de dados.
        
        Args:
            caminho_arquivo: Caminho para o arquivo CSV
            encoding: Codificação do arquivo (default: 'utf-8')
            sep: Separador do CSV (default: ',')
        """
        self.caminho_arquivo = path
        self.encoding = encoding
        self.sep = sep
        
    def load(self) -> pd.DataFrame:
        """
        Carrega o arquivo CSV e retorna um DataFrame.
        
        Returns:
            DataFrame com os dados carregados
            
        Raises:
            FileNotFoundError: Se o arquivo não existir
        """
        try:
           
            # Carrega o CSV
            self.df = pd.read_csv(
                self.caminho_arquivo, 
                encoding=self.encoding, 
                sep=self.sep
            )
            
            print(f" Arquivo carregado com sucesso!")
            print(f" Dimensões: {self.df.shape[0]} linhas x {self.df.shape[1]} colunas")
            print(f" Colunas: {list(self.df.columns)}")
            
            return self.df
            
        except FileNotFoundError as e:
            print(f" Erro: {e}")
            raise