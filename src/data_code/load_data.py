from features.data_frame import DF

class LoadData:
    def __init__(self, path: str):
        self.path = path
    
    def load(self, **kwargs) -> DF:
        """
        Load a file and return its contents as a custom DataFrame object.

        This method reads data from various file formats and returns an instance of the custom 
        DataFrame class, which inherits from pandas.DataFrame.

        Supported file formats:
            - .csv (Comma-separated values)
            - .xlsx (Microsoft Excel)
            - .json (JavaScript Object Notation)

        Returns:
            CustomDataFrame: An instance of the custom DataFrame class containing the data 
                            from the input file. This class inherits all functionality from 
                            pandas.DataFrame with additional custom features.

        Raises:
            ValueError: If the file format is not supported or the file cannot be read.
        """
        if self.path.endswith('.csv'):
            df = DF.read_csv(self.path, **kwargs)
        elif self.path.endswith('.xlsx'):
            df = DF.read_excel(self.path, **kwargs)
        elif self.path.endswith('.json'):
            df = DF.read_json(self.path, **kwargs)
        else:
            raise ValueError(f" Unsupported file: {self.path}")
        return df 