import pandas as pd
import os


class DF(pd.DataFrame):
    """
    Custom DataFrame class inheriting from pandas.DataFrame.

    This class extends pandas.DataFrame with additional functionality and custom methods
        while maintaining all the standard pandas DataFrame features and behaviors.

    Inherits from:
    pandas.DataFrame: All methods and attributes of pandas.DataFrame are available.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def read_csv(cls, path, **kwargs):
        """
        Read a CSV file and return as a custom DataFrame.

        This method reads a CSV file using pandas.read_csv() and converts the result to an
            instance of the custom DataFrame class.

        Args:
        path (str): Path to the CSV file (local path or URL)
        **kwargs: Additional keyword arguments passed directly to pandas.read_csv()
                Common options include:
                - encoding: File encoding (default: 'utf-8')
                - sep: Delimiter to use (default: ',')
                - header: Row number to use as column names (default: 0)
                - index_col: Column to use as row index
                - usecols: Columns to read (by position or name)

        Returns:
            CustomDataFrame: An instance of the custom DataFrame class containing the CSV data.
        """
        # 1. Lê o CSV usando pandas
        df_pandas = pd.read_csv(path, **kwargs)
        
        # 2. Converte para DF personalizado e retorna
        return cls(df_pandas)

    @classmethod
    def read_exel(cls, path, **kwargs):
        """
        Read an Excel file and return as a custom DataFrame.

        This method reads an Excel file using pandas.read_excel() and converts the result to an
            instance of the custom DataFrame class, which inherits from pandas.DataFrame.
        Supports both .xls and .xlsx formats.

        Args:
            path (str): Path to the Excel file (.xls or .xlsx)
            **kwargs: Additional keyword arguments passed directly to pandas.read_excel()
                    Common options include:
                    - sheet_name: Sheet to read (can be string for sheet name, 
                                int for sheet index, or None for all sheets)
                                Default: 0 (first sheet)
                    - header: Row number to use as column names (default: 0)
                    - skiprows: Number of rows to skip at the beginning (int) or 
                                list of row indices to skip
                    - nrows: Number of rows to read (after skipping)
                    - usecols: Columns to read (by letter, name, or index)
                    - dtype: Data type for columns (dict of column name -> type)

        Returns:
            CustomDataFrame: An instance of the custom DataFrame class containing the Excel data.
                            If multiple sheets are read (sheet_name=None), returns a dict of
                            {sheet_name: CustomDataFrame}.
        """
        # 1. Lê o CSV usando pandas
        df_pandas = pd.read_excel(path, **kwargs)
        
        # 2. Converte para DF personalizado e retorna
        return cls(df_pandas)

    @classmethod
    def read_json(cls, path, **kwargs):
        """
        Read a JSON file and return as a custom DataFrame.

        This method reads a JSON file using pandas.read_json() and converts the result to an
            instance of the custom DataFrame class, which inherits from pandas.DataFrame.
        Supports various JSON formats and structures.

        Args:
            path (str): Path to the JSON file (.json or .jsonl)
            **kwargs: Additional keyword arguments passed directly to pandas.read_json()
                    Common options include:
                    - orient: Expected JSON format. Options:
                            'records' : list of dicts like [{col: val}, ...]
                            'index' : dict like {index: {col: val}}
                            'columns' : dict like {col: {index: val}} (default)
                            'values' : array like [val1, val2, ...]
                            'table' : schema containing 'data' and 'index'
                    - encoding: File encoding (default: 'utf-8')
                    - lines: If True, read file as JSON lines format (.jsonl) with 
                            one JSON object per line (default: False)
                    - dtype: Data type for columns or dict of column name -> type
                    - convert_dates: List of columns to parse as dates (default: True)

        Returns:
            CustomDataFrame: An instance of the custom DataFrame class containing the JSON data.
        """
        # 1. Lê o JSON usando pandas
        df_pandas = pd.read_json(path, **kwargs)
        
        # 2. Converte para DF personalizado e retorna
        return cls(df_pandas)
    
    def _time_to_approved(self, path):
        """
        Calculate and save the time taken for order approval.
        
        This method processes order data to compute the time difference between purchase
            and approval, then saves the result as a CSV file.
        
        The method performs the following steps:
            1. Extracts relevant columns (order_id, customer_id, purchase_timestamp, approved_at)
            2. Removes rows with missing approval or purchase timestamps
            3. Calculates the time difference (in hours/days) between purchase and approval
            4. Saves the resulting DataFrame to a CSV file
        
        Args:
            path (str): Directory path where the output CSV file will be saved
            
        Returns:
            None: The result is saved directly to disk as 'time_to_approved.csv'
            
        Output file:
            - Filename: 'time_to_approved.csv'
            - Columns:
                - order_id: Unique order identifier
                - customer_id: Unique customer identifier  
                - order_purchase_timestamp: Timestamp when order was placed
                - order_approved_at: Timestamp when order was approved
                - time_to_approved: Calculated time difference (likely in hours/days)
        """
        columns = ['order_id', 'customer_id', 'order_purchase_timestamp', 'order_approved_at']
        time_to_approved = self._filter_and_prepare_dataframe(
            columns, drop_na_cols=['order_approved_at', 'order_purchase_timestamp'])

        time_to_approved = self._calculate_time_difference(
            time_to_approved,
            start_date_col='order_purchase_timestamp',
            end_date_col='order_approved_at',
            new_col_name='time_to_approved'
        )

        file_path = os.path.join(path, 'time_to_approved.csv')
        time_to_approved.to_csv(file_path, index=False)

    def _transit_time(self,path):
        '''
        Calculate and save the transit time for order deliveries.
    
        This method computes the time elapsed between an order being handed to the carrier
            and its final delivery to the customer. The result is saved as a CSV file for
            further analysis of delivery performance.
        
        The method performs the following steps:
            1. Extracts relevant columns (order_id, customer_id, carrier_date, delivery_date)
            2. Removes rows with missing carrier or delivery timestamps
            3. Calculates the transit time (difference between delivery and carrier dates)
            4. Saves the resulting DataFrame to a CSV file
        
        Args:
            path (str): Directory path where the output CSV file will be saved
            
        Returns:
            None: The result is saved directly to disk as 'transit_time.csv'
            
        Output file:
            - Filename: 'transit_time.csv'
            - Columns:
                - order_id: Unique order identifier
                - customer_id: Unique customer identifier
                - order_delivered_carrier_date: Timestamp when order was handed to carrier
                - order_delivered_customer_date: Timestamp when order was delivered to customer
                - transit_time: Calculated time difference (typically in hours/days)
                    representing the shipping duration
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
    
    def _comparasion_time(self,path):
        '''
        Compare actual delivery time against estimated delivery time.
    
        This method calculates the difference between the estimated delivery date
            and the actual delivery date to evaluate delivery performance and accuracy
            of delivery estimates. The result is saved as a CSV file.
    
        The calculation shows:
            - Positive values: Order arrived BEFORE the estimated date (early delivery)
            - Negative values: Order arrived AFTER the estimated date (late delivery)
            - Zero: Order arrived exactly on the estimated date
    
        The method performs the following steps:
            1. Extracts relevant columns (order_id, customer_id, estimated_date, actual_delivery_date)
            2. Removes rows with missing estimated or actual delivery timestamps
            3. Calculates the comparison time (estimated_date - actual_delivery_date)
            4. Saves the resulting DataFrame to a CSV file
        
        Args:
            path (str): Directory path where the output CSV file will be saved
        
        Returns:
            None: The result is saved directly to disk as 'comparation_time.csv'
            
        Output file:
            - Filename: 'comparation_time.csv'
            - Columns:
                - order_id: Unique order identifier
                - customer_id: Unique customer identifier
                - order_estimated_delivery_date: Estimated delivery date provided to customer
                - order_delivered_customer_date: Actual delivery date
                - comparation_time: Difference between estimated and actual delivery
                                (estimated_date - actual_date)
                                • Positive = Early delivery
                                • Negative = Late delivery
                                • Zero = On-time delivery
        '''

        columns = ['order_id', 'customer_id', 'order_estimated_delivery_date','order_delivered_customer_date']
        comparasion_time = self._filter_and_prepare_dataframe(
        columns, drop_na_cols=['order_estimated_delivery_date','order_delivered_customer_date'])
      
        comparasion_time = self._calculate_time_difference(
            comparasion_time,
            start_date_col='order_delivered_customer_date',
            end_date_col='order_estimated_delivery_date',
            new_col_name='comparasion_time')

        file_path = os.path.join(path, 'comparasion_time.csv')
        comparasion_time.to_csv(file_path, index=False)
    
    def _total_time(self,path):
        '''
        Calculate the complete order lifecycle time from purchase to delivery.
        
        This method computes the total time elapsed between the customer placing an order
            and the final delivery to their door. This represents the end-to-end order fulfillment
            cycle, including all intermediate steps (processing, approval, shipping, etc.).
            
        The method performs the following steps:
            1. Extracts relevant columns (order_id, customer_id, purchase_timestamp, delivery_date)
            2. Removes rows with missing purchase or delivery timestamps
            3. Calculates the total order lifecycle time
            4. Saves the resulting DataFrame to a CSV file
        
        Args:
            path (str): Directory path where the output CSV file will be saved
            
        Returns:
            None: The result is saved directly to disk as 'total_time.csv'
            
        Output file:
            - Filename: 'total_time.csv'
            - Columns:
                - order_id: Unique order identifier
                - customer_id: Unique customer identifier
                - order_purchase_timestamp: Timestamp when order was placed by customer
                - order_delivered_customer_date: Timestamp when order was delivered to customer
                - total_time: Complete order lifecycle duration
                    (delivery_date - purchase_date)
        '''

        columns = ['order_id', 'customer_id', 'order_purchase_timestamp','order_delivered_customer_date']
        total_time = self._filter_and_prepare_dataframe(
        columns, drop_na_cols=['order_purchase_timestamp','order_delivered_customer_date'])

        total_time = self._calculate_time_difference(
            total_time,

            start_date_col='order_purchase_timestamp',
            end_date_col='order_delivered_customer_date',
            new_col_name='total_time')

        file_path = os.path.join(path, 'total_time.csv')
        total_time.to_csv(file_path, index=False)
        
 
    def save_data(self, path, output = None):
        '''
        Save processed data metrics to CSV files.
    
        This method orchestrates the calculation and saving of various order fulfillment
            metrics. It can generate individual metrics or all available metrics based on
            the output parameter.
        
        The method serves as a unified interface for data export, calling the appropriate
            internal processing methods based on user selection.
        
        Args:
            path (str): Directory path where the CSV files will be saved
            output (str, optional): Specific metric to calculate and save.
                Available options:
                - "time_to_approved" : Time from order placement to approval
                - "transit_time"     : Time from carrier receipt to customer delivery
                - "comparation_time" : Difference between estimated and actual delivery
                - "total_time"       : Complete order lifecycle (purchase to delivery)
                - None (default)     : Saves ALL available metrics
            
        Returns:
            None: All results are saved directly to disk as CSV files
            
        Output files generated:
            When output is specified:
                - {path}/time_to_approved.csv (if output="time_to_approved")
                - {path}/transit_time.csv (if output="transit_time")
                - {path}/comparation_time.csv (if output="comparation_time")
                - {path}/total_time.csv (if output="total_time")
                
            When output=None (default):
                All four CSV files are generated in the specified directory:
                - time_to_approved.csv
                - transit_time.csv
                - comparation_time.csv
                - total_time.csv
        '''
        
        if output == "time_to_approved":
            self._time_to_approved(path)
        elif output == "transit_time":
            self._transit_time(path)
        elif output == "comparation_time":
            self._comparasion_time(path)
        elif output == "total_time":
            self._total_time(path)
        else:
            self._time_to_approved(path)
            self._transit_time(path)
            self._comparasion_time(path)
            self._total_time(path)


    def _filter_and_prepare_dataframe(self, columns, drop_na_cols=None):
        """
        Filter and prepare dataframe by removing canceled orders and missing values.
        
        This method performs essential data cleaning operations to ensure data quality
            before further processing. It applies two main filters:
                1. Removes all canceled orders from the dataset
                2. Removes rows with missing values (NaN) in specified columns
        
        The method is designed to be a reusable utility for preparing clean data
            for various metrics calculations throughout the pipeline.
        
        Args:
            columns (list): List of column names to select from the original dataframe.
                Only these columns will be included in the result.
                
            drop_na_cols (list, optional): List of columns to check for missing values.
                Rows with NaN in any of these columns will be removed.
                If None (default), uses all columns specified in 'columns' parameter.
        
        Returns:
            pd.DataFrame: A cleaned dataframe containing:
                - Only rows where order_status is not 'canceled'
                - Only the specified columns
                - No missing values in the specified columns (drop_na_cols)
                - A copy of the filtered data (original remains unchanged)
        
        Processing pipeline:
            1. Filter out canceled orders
                └─ Condition: order_status != 'canceled'
            2. Select relevant columns
                └─ Keeps only columns specified in 'columns' parameter
            3. Remove missing values
                └─ Drops rows with NaN in drop_na_cols (or all selected columns)
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
        Calculate time difference between two date columns.
        
        This core utility method computes the time elapsed between two dates/timestamps
            and adds the result as a new column. It's used across all metric calculations
            to ensure consistent date handling and difference computation.
        
        The method performs two main operations:
            1. Converts specified columns to datetime format (if not already)
            2. Calculates the difference: end_date - start_date
        
        Args:
            df (pd.DataFrame): Input dataframe containing the date columns
            start_date_col (str): Name of the column containing the start date/timestamp
            end_date_col (str): Name of the column containing the end date/timestamp
            new_col_name (str): Name for the new column that will store the time difference
        
        Returns:
            pd.DataFrame: The original dataframe with an additional column containing
                        the time difference. The new column contains pandas Timedelta
                        objects.
        """
                
        # Converter para datetime
        df[start_date_col] = pd.to_datetime(df[start_date_col])
        df[end_date_col] = pd.to_datetime(df[end_date_col])
        
        # Calcular diferença
        df[new_col_name] = df[end_date_col] - df[start_date_col]
        
        return df