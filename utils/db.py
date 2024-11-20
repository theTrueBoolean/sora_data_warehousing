
import os
import yaml
from sqlalchemy import create_engine


# Read config.yaml
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as file:
        conf = yaml.safe_load(file)
    return conf

config = load_config()

db_config = config["database"]
DATABASE_URL = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

engine = create_engine(DATABASE_URL)
