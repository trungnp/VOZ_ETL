from configparser import ConfigParser
import os
from airflow.models import Variable

cwd = os.getcwd()

FILE_PATH = "/Users/trungnp/Library/Mobile Documents/com~apple~CloudDocs/Study/DE/voz/plugins/utilities/conf.ini"


def config(filename=FILE_PATH, section=""):
    config = Variable.get(section, deserialize_json=True)
    # config = ConfigParser()
    # config.read(filename)

    print(config.values())

    if section == "s3":
        return {'AWS_ACCESS_KEY_ID': config['AWSAccessKeyId'],
                'AWS_SECRET_KEY': config['AWSSecretKey'],
                'BUCKET_NAME': config['BucketName']}
    elif section in ["postgresql", "postgresql_aws", "redshift"]:
        result = {'host': config['host'], 'database': config['database'],
                'user': config['user'], 'password': config['password'],
                'port': config['port']}
        if section == 'redshift':
            result['iam'] = config['iam']
        return result
    # elif section == "postgresql_aws":
    #     return {'host': config['postgresql_aws']['host'], 'user': config['postgresql_aws']['user'],
    #             'password': config['postgresql_aws']['password'], 'port': config['postgresql_aws']['port'],
    #             'database': config['postgresql_aws']['database']}
    else:
        pass
