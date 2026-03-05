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
    
    def _time_to_approved(self, path):
    # Preparar dados
        columns = ['order_id', 'customer_id', 'order_purchase_timestamp', 'order_approved_at']
        time_to_approved = self._filter_and_prepare_dataframe(
            columns, drop_na_cols=['order_approved_at', 'order_purchase_timestamp'])

        time_to_approved = self._calculate_time_difference(
            time_to_approved,
            start_date_col='order_purchase_timestamp',
            end_date_col='order_approved_at',
            new_col_name='time_to_approved'
        )
        
        # Salvar
        file_path = os.path.join(path, 'time_to_approved.csv')
        time_to_approved.to_csv(file_path, index=False)

    def _transit_time(self,path):
        '''
        This method calculetes the time it takes for an order to be delivered,
        by calculating the difference between the order_delivered_customer_dat and order_purchase_timestamp columns.
        '''

        columns = ['order_id', 'customer_id', 'order_delivered_carrier_date', 'order_delivered_customer_date']
        transit_time = self._filter_and_prepare_dataframe(
        columns, drop_na_cols=['order_delivered_carrier_date', 'order_delivered_customer_date'])

        transit_time = self._calculate_time_difference(
            transit_time,
            start_date_col='order_delivered_customer_date',
            end_date_col='order_delivered_carrier_date',
            new_col_name='transit_time'
        )

        file_path = os.path.join(path, 'transit_time.csv')
        transit_time.to_csv(file_path, index=False)
    
    def _comparation_time(self,path):
        '''
        This method compares the time it take to make the delivered 
        and the estimate delivery time, calculating the difference between the order_delivered_customer_dat 
        and order_estimated_delivery_dat columns.
        '''

        columns = ['order_id', 'customer_id', 'order_estimated_delivery_date','order_delivered_customer_date']
        comparation_time = self._filter_and_prepare_dataframe(
        columns, drop_na_cols=['order_estimated_delivery_date','order_delivered_customer_date'])
      
        comparation_time = self._calculate_time_difference(
            comparation_time,
            start_date_col='order_delivered_customer_date',
            end_date_col='order_estimated_delivery_date',
            new_col_name='comparation_time')

        file_path = os.path.join(path, 'comparation_time.csv')
        comparation_time.to_csv(file_path, index=False)
    
    def _total_time(self,path):
        '''
        This method calculates the total time of an order, 
        by calculating the difference between the order_delivered_customer_dat 
        and order_purchase_timestamp columns.
        '''

        columns = ['order_id', 'customer_id', 'order_purchase_timestamp','order_delivered_customer_date']
        total_time = self._filter_and_prepare_dataframe(
        columns, drop_na_cols=['order_purchase_timestamp','order_delivered_customer_date'])

        total_time = self._calculate_time_difference(
            total_time,
            start_date_col='order_delivered_customer_date',
            end_date_col='order_purchase_timestamp',
            new_col_name='total_time')

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


    def _filter_and_prepare_dataframe(self, columns, drop_na_cols=None):
        """
        Método genérico para filtrar e preparar dados
        
        Parameters:
        -----------
        columns : list
            Lista de colunas a serem selecionadas
        drop_na_cols : list, optional
            Lista de colunas para remover NAs (se None, usa todas as colunas selecionadas)
        
        Returns:
        --------
        pd.DataFrame
            DataFrame filtrado e preparado
        """
        # Filtrar pedidos não cancelados
        df_filtered = self[self['order_status'] != 'canceled'].copy()
        
        # Selecionar colunas relevantes
        result_df = df_filtered[columns]
        
        # Remover NAs se especificado
        if drop_na_cols is None:
            drop_na_cols = columns
        result_df = result_df.dropna(subset=drop_na_cols)
        
        return result_df
    
    def _calculate_time_difference(self, df, start_date_col, end_date_col, new_col_name):
        """
        Calcula a diferença de tempo entre duas colunas de data
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame com os dados
        start_date_col : str
            Nome da coluna com a data inicial
        end_date_col : str
            Nome da coluna com a data final
        new_col_name : str
            Nome da nova coluna que receberá a diferença
        
        Returns:
        --------
        pd.DataFrame
            DataFrame com a nova coluna de diferença de tempo
        """
                
        # Converter para datetime
        df[start_date_col] = pd.to_datetime(df[start_date_col])
        df[end_date_col] = pd.to_datetime(df[end_date_col])
        
        # Calcular diferença
        df[new_col_name] = df[end_date_col] - df[start_date_col]
        
        return df