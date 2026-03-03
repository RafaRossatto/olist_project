import pandas as pd
import os


class DF(pd.DataFrame):
    """
    Classe personalizada que herda de pandas.DataFrame
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
        """
        # 1. Lê o JSON usando pandas
        df_pandas = pd.read_json(path, **kwargs)
        
        # 2. Converte para DF personalizado e retorna
        return cls(df_pandas)
    
    def _time_to_approved(self,path):
        '''
        This method calculates the time it take for an orden to be 
        aproved, by calculeting the diferrence between the order_aproved_at and order purchase_timestamp columns.
        '''


        df_filtred = self[self['order_status'] != 'canceled'].copy()

        # Selecionar colunas relevantes e remover NAs
        time_to_approved = df_filtred[['order_id', 'customer_id', 'order_purchase_timestamp', 'order_approved_at']].copy()
        time_to_approved = time_to_approved.dropna(subset=['order_approved_at', 'order_purchase_timestamp'])
        time_to_approved['order_purchase_timestamp'] = pd.to_datetime(time_to_approved['order_purchase_timestamp'])
        time_to_approved['order_approved_at'] = pd.to_datetime(time_to_approved['order_approved_at'])
        time_to_approved['time_to_aproved'] = time_to_approved['order_approved_at'] - time_to_approved['order_purchase_timestamp']
        file_path = os.path.join(path, 'time_to_approved.csv')
        time_to_approved.to_csv(file_path, index=False)

    def _transit_time(self,path):
        '''
        This method calculetes the time it takes for an order to be delivered,
        by calculating the difference between the order_delivered_customer_dat and order_purchase_timestamp columns.
        '''

        df_filtred = self[self['order_status'] != 'canceled'].copy()
        
        transit_time = df_filtred[['order_id', 'customer_id', 'order_delivered_carrier_date', 'order_delivered_customer_date']].copy()
        transit_time = transit_time.dropna(subset=['order_delivered_carrier_date', 'order_delivered_customer_date'])
        transit_time['order_delivered_customer_date'] = pd.to_datetime(transit_time['order_delivered_customer_date'])
        transit_time['order_delivered_carrier_date'] = pd.to_datetime(transit_time['order_delivered_carrier_date'])
        transit_time['transit_time'] = transit_time['order_delivered_customer_date'] - transit_time['order_delivered_carrier_date']
        
        file_path = os.path.join(path, 'transit_time.csv')
        transit_time.to_csv(file_path, index=False)
    
    def _comparation_time(self,path):
        '''
        This method compares the time it take to make the delivered 
        and the estimate delivery time, calculating the difference between the order_delivered_customer_dat 
        and order_estimated_delivery_dat columns.
        '''

        df_filtred = self[self['order_status'] != 'canceled'].copy()
        
        comparation_time = df_filtred[['order_id', 'customer_id','order_estimated_delivery_date','order_delivered_customer_date']].copy()
        comparation_time = comparation_time.dropna(subset=['order_estimated_delivery_date','order_delivered_customer_date'])
        comparation_time['order_delivered_customer_date'] = pd.to_datetime(comparation_time['order_delivered_customer_date'])
        comparation_time['order_estimated_date'] = pd.to_datetime(comparation_time['order_estimated_delivery_date'])
        comparation_time['comparation_time'] = comparation_time['order_estimated_date'] - comparation_time['order_delivered_customer_date']
        
        file_path = os.path.join(path, 'comparation_time.csv')
        comparation_time.to_csv(file_path, index=False)
    
    def _total_time(self,path):
        '''
        This method calculates the total time of an order, 
        by calculating the difference between the order_delivered_customer_dat 
        and order_purchase_timestamp columns.
        '''

        df_filtred = self[self['order_status'] != 'canceled'].copy()
        
        total_time = df_filtred[['order_id', 'customer_id','order_purchase_timestamp','order_delivered_customer_date']].copy()
        total_time = total_time.dropna(subset=['order_purchase_timestamp','order_delivered_customer_date'])
        total_time['order_delivered_customer_date'] = pd.to_datetime(total_time['order_delivered_customer_date'])
        total_time['order_purchase_timestamp'] = pd.to_datetime(total_time['order_purchase_timestamp'])
        total_time['total_time'] = total_time['order_delivered_customer_date'] - total_time['order_purchase_timestamp']
        
        file_path = os.path.join(path, 'total_time.csv')
        total_time.to_csv(file_path, index=False)
        
 
    def save_data(self, path, output = None):
        '''
        This method saves the processed data, using the other methods.
        '''
        
        if output == "time_to_approved":
            self._time_to_approved(path)
        elif output == "transit_time":
            self._transit_time(path)
        elif output == "comparation_time":
            self._comparation_time(path)
        elif output == "total_time":
            self._total_time(path)
        else:
            self._time_to_approved(path)
            self._transit_time(path)
            self._comparation_time(path)
            self._total_time(path)
