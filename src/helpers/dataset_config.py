import json


class DatasetConfig:
    """
    Defining a dataset config values
    """
    def __init__(self,
                 config_file_path):

        with open(config_file_path, 'r') as config_file:
            config=json.loads(config_file.read())

        self.name = config['name']
        self.format = config['format']
        self.path = config['path']
        self.columns = config['columns']
        self.data_type_options = config['data_type_options']
