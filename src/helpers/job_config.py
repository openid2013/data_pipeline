import json


class JobConfig:
    """
    Defining a job config values
    """
    def __init__(self,
                 config_file_path):

        with open(config_file_path, 'r') as config_file:
            config=json.loads(config_file.read())

        self.name = config['name']
        self.yarn_endpoint = config['yarn_endpoint']
        self.one_file_output = config.get('one_file_output', None)
        self.output_path = config['output_path']
