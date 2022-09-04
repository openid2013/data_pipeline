unit-tests:
	python3 -m unittest discover

drugs-reports-job-local-run:
	python3 src/drugs_reports_job.py\
		--job_config="./config/local/job/drugs_reports_job.config" \
		--drugs_config="./config/local/dataset/drugs.config" \
		--pubmed_csv_config="./config/local/dataset/pubmed_csv.config" \
		--pubmed_json_config="./config/local/dataset/pubmed_json.config" \
		--clinical_trials_config="./config/local/dataset/clinical_trials.config";

journal-with-most-drugs-job-local-run:
	python3 src/journal_with_most_drugs_job.py \
    	--drugs_reports_job_config="./config/local/job/drugs_reports_job.config" \
    	--job_config="./config/local/job/journal_with_most_drugs_job.config";

validate-drugs-dataset-job:
	python3 src/validate_dataset_job.py \
		--data_source_config="./config/local/dataset/drugs.config";

setup-local-env:
	python3 -m venv venv
	source venv/bin/activate
	pip install -r requirements.txt
