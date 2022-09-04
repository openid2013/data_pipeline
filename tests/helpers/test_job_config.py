import unittest

from src.helpers.job_config import JobConfig


class TestJobConfig(unittest.TestCase):
    def test_init(self):
        config = JobConfig("./tests/data/drugs_reports_job.config")

        assert(config.name == "drugs_reports_job")
        assert(config.yarn_endpoint == "local[1]")
        assert config.one_file_output
        assert(config.output_path == "./data/result/drugs_reports.json")


if __name__ == '__main__':
    unittest.main()
