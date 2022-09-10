import redshift_connector
from sqlalchemy import create_engine
from src.utilities.config import config


def connect(section=""):
    """ Connect to postgresql or redshift"""
    params = config(section=section)
    print(f'Connecting to section {section}...')
    if section in ['postgresql', 'postgresql_aws']:
        engine = create_engine(
            f'postgresql://{params["user"]}:{params["password"]}@{params["host"]}:5432/{params["database"]}')
    elif section == 'redshift':
        engine = redshift_connector.connect(
            host=params['host'],
            database=params['database'],
            user=params['user'],
            password=params['password'],
            port=int(params['port']),
        )
    else:
        print(f'Connect to section {section} failed...')
        return None

    print(f'{section} connection established!')

    return engine


# connect(section="postgresql_aws")