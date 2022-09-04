import unittest

from src.helpers.dataset_config import DatasetConfig


class TestDatasetConfig(unittest.TestCase):
    def test_init(self):
        config = DatasetConfig("./tests/data/clinical_trials.config")

        assert(config.name == "clinical_trials")
        assert(config.format == "csv")
        assert(config.columns == [
            "id",
            "scientific_title",
            "date",
            "journal"
        ])
        assert(config.data_type_options == {
            "header": "True"
        })
        assert(config.path == "./tests/data/clinical_trials.csv")


if __name__ == '__main__':
    unittest.main()
