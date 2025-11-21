import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os 
import logging 
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()



DB_USER=os.getenv("DB_USER")
DB_PW=os.getenv("DB_PW")
DB_HOST=os.getenv("DB_HOST")
DB_PORT=os.getenv("DB_PORT")
DB_NAME=os.getenv("DB_NAME")


def _make_engine():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PW}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        with engine.connect():
            logger.info("Conexão bem sucedida")
        return engine
    except Exception as e:
        logger.error(f"Erro de conexão: {e}")
        raise e


def send_csv_to_db(engine, input_filepath:Path, table_name:str, _schema:str):
    input_dir = Path(input_filepath)
    ingestion_date = datetime.now()
    df = pd.read_csv(input_dir,sep=',',dtype=str)
    df['load_date'] = ingestion_date
    try:
        df.to_sql(name=table_name, con=engine,schema=_schema,if_exists='replace',index=False)
        logger.info("Carga realizada!")
    except Exception as e:
        logger.error(f"Erro na carga: {e}")

def run_ingestion_to_raw():
    engine = _make_engine()

    files_to_ingest = {
        "raw_bi_funnel_email": "data/raw/funnel_mail/bi_challenge_rd_bi_funnel_email.csv",
        "raw_metas_email": "data/raw/metas/metas_email.csv",
    }

    for table, file in files_to_ingest.items():
        send_csv_to_db(engine, Path(file), table, "raw")


if __name__ == "__main__":
    run_ingestion_to_raw()



