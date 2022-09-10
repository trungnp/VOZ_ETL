from configparser import ConfigParser

FILE_PATH = "/Users/trungnp/Library/Mobile Documents/com~apple~CloudDocs/Study/DE/voz/conf.ini"


def config(filename=FILE_PATH, section=""):
    '''Get authentication params'''

    config = ConfigParser()
    config.read(filename)

    if section == "s3":
        return {'AWS_ACCESS_KEY_ID': config['s3']['AWSAccessKeyId'],
                'AWS_SECRET_KEY': config['s3']['AWSSecretKey'],
                'BUCKET_NAME': config['s3']['BucketName']}
    elif section in ["postgresql", "postgresql_aws", "redshift"]:
        result = {'host': config[section]['host'], 'database': config[section]['database'],
                'user': config[section]['user'], 'password': config[section]['password'],
                'port': config[section]['port']}
        if section == 'redshift':
            result['iam'] = config[section]['iam']
        return result
    else:
        return None
