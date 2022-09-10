import redshift_connector
from sqlalchemy import create_engine
from utilities.config import config


def connect(section=""):
    """ Connect to the PostgreSQL database server """
    params = config(section=section)
    print(f'Connecting to section {section}...')
    # conn = psycopg2.connect(**db)
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


# if __name__ == '__main__':
#     connect()