# README

## I) Python et Data Engineering

### Code Base
Voici la structure du projet:
- `./config/${env}/`: organisé par environnement, et contient des fichiers de config de Job / Dataset.
- `./data/`: contient les données brutes, et les résultats des traitements
- `./src/`:
    - contient les jobs (suffixés par *_job.py), qui vont constituer notre pipeline data, et qui seront ordonnancés par un Orchestrator.
    - contient le package `helpers`, qui contient un ensemble de modules ré-utilisables.
- `./tests/`: couvre les tests unitaires.

Il y'a 2 jobs identifiés qui vont définir notre pipeline data:
- `validate_dataset_job.py`:
    - ce job est destiné à valider certains aspects techniques lié à la donnée:
        - schema du fichier en entré
        - format du fichier
        - contenu de la donnée n'est pas corrompu.
    - un job de validation est prévu par dataset, pour le cas de notre pipeline, on aura 4 jobs de validations:
        - Un pour clinical_trials.csv
        - Un pour drugs.csv
        - un pour pubmed.csv
        - et un pour pubmed.json
    - si aucune exception n'est déclenchée, on passe à l'étape suivante du pipeline.
    - ce Job pourrait implémenter des règles de validation business sur les données (columns non nullable, data type réspecté ...), ou sur un job séparré. (non implémenté dans cette version)
    
    - pour le cas du fichier 'pubmed.json' livré dans le livrable du test, le contenu JSON est corrompu, 
    le job de validation va amener à cracher. 
    Une demande va être faite au Data Provider, pour re-livrer une nouvelle version propre du fichier.
    Une nouvelle version du fichier a été directement placé dans `./data/raw.pubmed.json`.
        
    - lignes de commandes pr lancer les 4 jobs de validation:
    ```bash
        $ python3 src/validate_dataset_job.py \
		    --data_source_config="./config/local/dataset/drugs.config";
        $ python3 src/validate_dataset_job.py \
		    --data_source_config="./config/local/dataset/clinical_trials.config";
        $ python3 src/validate_dataset_job.py \
		    --data_source_config="./config/local/dataset/pubmed_csv.config";
        $ python3 src/validate_dataset_job.py \
		    --data_source_config="./config/local/dataset/pubmed_json.config";
    ```

- `drugs_reports_job.py`:
    - le job s'exécute juste après l'étape de validation des données sources.
    - le job génère un ensemble de liaisons. Une liaison est constitué de:
        - réference de drug
        - article qui mentionne le drug (soit Pubmed ou Clinical Trials)
        - le journal qui a émis l'article
        - la date de publication
    - la modélisation choisie est une représentation applatie, dénormalisée, 
    simple, et efficace de la donnée, déstinée à couvrir la plus part des uses cases
    y compris le use case qui nous a été demandé d'implementé, sans avoir recours à
    aller chercher un complement d'information, via des requêtes supplémentaires.
    
    - le job contient une étape d'uniformization des données sources, qui concerne le formattage des dates. 
    L'implementation proposée est couplée au job de traitement. 
    On pourrait envisager un job séparré, qui pourrait persister une version de la donnée uniformisée 
    où il y'a plus de règles d'uniformisations, pour eviter de faire le même traitement dans differents projets / équipes.
     
    - le parametre one-file-output, du fichier de config du job `./config/local/job/drugs_reports_job.config` permet 
    de générer une liste de liaisons en un seul fichier JSON, simple à consommer.
    Dans le cadre des données volumineuses, ce paramètre est amené à être désactivé, comme le traitement est distribué.
    - l'output du job: `./data/result/drugs_reports.json`
    - lancer le job en local:
    ```bash
        $ python3 src/drugs_reports_job.py\
            --job_config="./config/local/job/drugs_reports_job.config" \
            --drugs_config="./config/local/dataset/drugs.config" \
            --pubmed_csv_config="./config/local/dataset/pubmed_csv.config" \
            --pubmed_json_config="./config/local/dataset/pubmed_json.config" \
            --clinical_trials_config="./config/local/dataset/clinical_trials.config";
    ```

### Traitement ad-hoc
- l'output du job: `./data/result/journal_with_most_drugs.json`
- lancer le job en local:
```bash
    $ python3 src/journal_with_most_drugs_job.py \
    	--drugs_reports_job_config="./config/local/job/drugs_reports_job.config" \
    	--job_config="./config/local/job/journal_with_most_drugs_job.config";
```

### les élements à prendre en considération pour gérer de grosses volumétries
Le framework de calcul utilisé Apache Spark, fonctionne à la fois sur des datasets de petites taille, comme sur des données volumineuses,
et le runtime est scallable, il peut tourner sur une machine, comme sur plusieurs.
Ceci dit, il y'a quasi aucun changement à faire sur le code base, hormis ces elements:
-   Prendre en considération le sizing de l'infrastructure, ainsi que les ressourses à allouer sur chacune des machines 
pour supporter la charge du traitement des données volumineuses.
-   Stocker les datasets et les résultats des jobs dans un système de stockage hautement disponible, et fiable  (Cloud, ou Onpremise),
avec un partitionning des données à bases de date de réception / génération, pr une meilleur organisation.
-   créer des nouveaux fichiers de configs de jobs pour l'environment target, et revoir certaines valeurs:
    -   `yarn_endpoint`: pointer le job Spark sur le nouveau Cluster.
    -   `one_file_output`: le passer à false
    -   `output_path`: mettre un path vers un système de fichier distribué.



## II) SQL
### Requête 1

```
SELECT 
    FORMAT_DATE("%d/%m/%Y", date) AS date,
    SUM(prod_price * prod_qty) AS ventes
FROM transaction
WHERE date BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY date
ORDER BY date;
```

### Requête 2

#### Using PIVOT() on BigQuery
```
WITH client_detailed_products AS (
  SELECT 
      t.client_id,
      p.product_type,
      (t.prod_price * t.prod_qty) AS total_price
  FROM transaction t, product_nomenclature p
  WHERE t.prod_id = p.product_id
    AND t.date BETWEEN '2020-01-01' AND '2020-12-31'
)
SELECT * FROM client_detailed_products
PIVOT(
  SUM(total_price) as ventes
  FOR product_type in ('meuble', 'deco')
);
```

#### Without using PIVOT()
```
WITH client_detailed_products AS (
  SELECT 
      t.client_id,
      p.product_type,
      (t.prod_price * t.prod_qty) AS total_price
  FROM transaction t, product_nomenclature p
  WHERE t.prod_id = p.product_id
    AND t.date BETWEEN '2020-01-01' AND '2020-12-31'
)
SELECT 
  client_id,
  SUM(CASE product_type WHEN "meuble" THEN total_price ELSE 0 END) AS ventes_meuble,
  SUM(CASE product_type WHEN "deco" THEN total_price ELSE 0 END) AS ventes_deco
FROM client_detailed_products
GROUP BY client_id;
```
