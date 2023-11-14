from google.cloud import bigquery 
from google.oauth2 import service_account
import os
import json
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(os.path.abspath(__file__)[:-11]) / '.env'
load_dotenv(dotenv_path=env_path)

## BQ

class bq:

    def __init__(self):
        """
        Laster inn serviceAccount credentials via lokal .env fil. Sett opp en environmentvariabel med credentials til service account navngitt 'serviceAccount', eller lag din egen og erstatt referansen under. 
        
        Ta kontakt med Hallvard.lid@nrk.no eller Eirik.brautaset@nrk.no for å få kreditering.
        """
        self.credentials = service_account.Credentials.from_service_account_info(json.loads(os.environ['serviceAccount']))
        self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)

    def test(self):
        try:
            q = self.client.query("SELECT 1")
            d = q.result()
            return "nrk-bq-wrapper vellykket"
        except Exception as e:
            return e

    def query(self, sql_query):
        try:
            return self.client.query(sql_query).to_dataframe()
        except Exception as e:
            print(e)

    def listDataset(self):
        datasets = list(self.client.list_datasets())  # Make an API request.
        project = self.client.project

        if datasets:
            print("Datasets i prosjektet {}:".format(project))
            for dataset in datasets:
                print("\t{}".format(dataset.dataset_id))
        else:
            print("{} Prosjektet har ikke noen datasett.".format(project))
    
    def listSchemas(self, dataset):
        pass
        



if __name__ == "__main__":
    print(bq.test())
