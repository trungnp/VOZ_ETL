import json
from io import StringIO

import sqlalchemy


def load_user_staging_to_production(engine, staging_table: str, prod_table: str):
    """Load users data from staging table to production table"""
    total_rows_old = 0
    inspect = sqlalchemy.inspect(engine)
    with engine.begin() as conn:
        if inspect.has_table(prod_table):
            delete_duplicate = f"DELETE FROM {prod_table} USING {staging_table} WHERE {prod_table}.user_id = {staging_table}.user_id;"
            total_rows_old = conn.execute(f"SELECT count(user_id) FROM {prod_table}").fetchone()[0]
            conn.execute(delete_duplicate)
        else:
            create_table_users = f'''
            CREATE TABLE IF NOT EXISTS {prod_table} (  
                                                    user_id INT PRIMARY KEY,  
                                                    username VARCHAR(100) NOT NULL,
                                                    title VARCHAR(100) NOT NULL,
                                                    location VARCHAR(100),
                                                    join_date DATE NOT NULL,
                                                    last_seen TIMESTAMP DEFAULT NOW(),
                                                    message INT DEFAULT 0,
                                                    reaction INT DEFAULT 0,
                                                    point INT DEFAULT 0,                                                                                                        
                                                    created_at TIMESTAMP DEFAULT NOW(),
                                                    updated_at TIMESTAMP DEFAULT NOW()
                                                );
            '''
            conn.execute(create_table_users)
        conn.execute(f"INSERT INTO {prod_table} (SELECT * FROM {staging_table});")
        conn.execute(f"DROP TABLE IF EXISTS {staging_table};")
        total_rows_new = conn.execute(f"SELECT count(user_id) FROM {prod_table}").fetchone()[0]
        print(f"Loaded {total_rows_new - total_rows_old} new rows from staging to production successfully!")
        print(f"Deleted table {staging_table} successfully!")


def load_user_to_staging(engine, table: str, df):
    """Load users data as a DataFrame into staging table"""
    create_table = f''' 
        DROP TABLE IF EXISTS {table};
        CREATE TABLE {table} (    
                                user_id INT PRIMARY KEY,  
                                username VARCHAR(100) NOT NULL,
                                title VARCHAR(100) NOT NULL,
                                location VARCHAR(100),
                                join_date DATE NOT NULL,
                                last_seen TIMESTAMP DEFAULT NOW(),
                                message INT DEFAULT 0,
                                reaction INT DEFAULT 0,
                                point INT DEFAULT 0,                                                                                                        
                                created_at TIMESTAMP DEFAULT NOW(),
                                updated_at TIMESTAMP DEFAULT NOW()
                                );
        '''
    engine.execute(create_table)
    copy_df_to_postgresdb(engine, table, df)


def copy_df_to_postgresdb(engine, table: str, df):
    raw_conn = engine.raw_connection()
    cur = raw_conn.cursor()
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False, sep='\t')
    buffer.seek(0)
    try:
        cur.copy_from(buffer, table, sep='\t', null='')
        raw_conn.commit()
        print(f"Copied {df.shape[0]} rows into table {table} successfully!")
    except Exception as e:
        print(e)
    finally:
        cur.close()
        raw_conn.close()


def load_thread_staging_to_production(engine, staging_table: str, prod_table: str):
    """Load threads from staging table to production table"""
    with engine.begin() as conn:
        total_rows_old = 0
        inspect = sqlalchemy.inspect(engine)
        if inspect.has_table(prod_table):
            delete_duplicate_thread = f'''DELETE FROM {prod_table} USING {staging_table}
                                      WHERE {prod_table}.thread_id = {staging_table}.thread_id 
                                      AND jsonb_array_length({prod_table}.participants) IS NULL;                                      
                                      '''
            dont_delete_participant = f'''DELETE FROM {staging_table} USING {prod_table}
                                      WHERE {prod_table}.thread_id = {staging_table}.thread_id 
                                      AND jsonb_array_length({prod_table}.participants) IS NOT NULL;
                                      '''
            total_rows_old = conn.execute(f"SELECT count(thread_id) FROM {prod_table}").fetchone()[0]
            conn.execute(delete_duplicate_thread)
            conn.execute(dont_delete_participant)
        else:
            create_table_threads = f'''CREATE TABLE IF NOT EXISTS {prod_table} (    
                                    thread_id INT PRIMARY KEY,
                                    title TEXT NOT NULL,
                                    author_name VARCHAR(100) NOT NULL,
                                    author_id INT NOT NULL,
                                    reply_count VARCHAR(10) DEFAULT 0,
                                    view_count VARCHAR(10) DEFAULT 0,
                                    participants JSONB,
                                    created_at TIMESTAMP DEFAULT NOW(),                                                                                                                 
                                    updated_at TIMESTAMP DEFAULT NOW(),
                                    f int,
                                    status JSONB
                                    );
                            '''
            conn.execute(create_table_threads)

        conn.execute(f"INSERT INTO {prod_table} (SELECT * FROM {staging_table});")
        conn.execute(f"DROP TABLE IF EXISTS {staging_table};")
        total_rows_new = conn.execute(f"SELECT count(thread_id) FROM {prod_table}").fetchone()[0]
        print(f"Loaded {total_rows_new - total_rows_old} new rows from threads_staging to production successfully!")
        print(f"Deleted table {staging_table} successfully!")


def load_thread_to_staging(engine, threads_staging, data: dict):
    """Load thread as a json file into staging table"""
    create_table_threads = f'''DROP TABLE IF EXISTS {threads_staging};
                                CREATE TABLE IF NOT EXISTS {threads_staging} (    
                                    thread_id INT PRIMARY KEY,
                                    title TEXT NOT NULL,
                                    author_name VARCHAR(100) NOT NULL,
                                    author_id INT NOT NULL,
                                    reply_count VARCHAR(10) DEFAULT 0,
                                    view_count VARCHAR(10) DEFAULT 0,
                                    participants JSONB,
                                    created_at TIMESTAMP DEFAULT NOW(),                                                                                                                 
                                    updated_at TIMESTAMP DEFAULT NOW(),
                                    f int,
                                    status JSONB
                                    );
                                '''
    with engine.begin() as conn:
        conn.execute(create_table_threads)
        # load_json_to_thread_table(conn, threads_staging, json)
        query = f'''
                    INSERT INTO {threads_staging} SELECT * FROM jsonb_populate_recordset(NULL::{threads_staging}, %s);
                    '''
        conn.execute(query, (json.dumps(data),))
        print(f"Loaded {len(data)} rows to table {threads_staging} successfully!")

# def load_json_to_thread_table(engine, table, data: dict):
#     # with engine.connect() as conn:
#     query = f'''
#             INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, %s);
#             '''
#     engine.execute(query, (json.dumps(data),))
#     print(f"Loaded {len(data)} rows to table {table} successfully!")

#
# params = config(section="postgresql_aws")
# conn_string = f'postgresql://{params["user"]}:{params["password"]}@{params["host"]}:{params["port"]}/voz_db'
# engine = create_engine(conn_string)
# with open("../tmp6.json", "r") as fp:
#     test = json.load(fp)
#
# for i in range(len(test)):
#     test[i]['f'] = 33
#     test[i]['updated_at'] = datetime.now().strftime("%b %d, %Y %I:%M %p")
# load_thread_to_staging(engine, "test_staging", test)
# load_thread_staging_to_production(engine, staging_table="test_staging", prod_table="test")
