
def load_s3_to_redshift(engine, filepath, iam, table):
    '''Load file from {filepath} to table {table} on redshift with {iam} policy'''

    create_table = {'participants': f'''
        CREATE TABLE IF NOT EXISTS {table} (
            thread_id INT NOT NULL DISTKEY SORTKEY,
            user_id INT,
            username TEXT,
            reply TEXT,
            created_at TIMESTAMP,
            love INT,
            brick INT            
        )
    ''', 'threads_staging': f'''        
        CREATE TABLE IF NOT EXISTS {table} (    
        thread_id INT NOT NULL DISTKEY,
        title TEXT NOT NULL,
        author_name VARCHAR(100) NOT NULL,
        author_id INT NOT NULL,
        reply_count INT DEFAULT 0,
        view_count INT DEFAULT 0,                
        created_at TIMESTAMP DEFAULT NOW(),                                                                                                                 
        updated_at TIMESTAMP DEFAULT NOW(),
        f INT,
        status SUPER,
        word_count SUPER       
        );
    ''', 'users_staging': f'''
        CREATE TABLE IF NOT EXISTS {table} (  
            user_id INT NOT NULL DISTKEY,  
            username VARCHAR(100) NOT NULL,
            title VARCHAR(100) NOT NULL,
            location VARCHAR(100),
            join_date DATE NOT NULL SORTKEY,
            last_seen TIMESTAMP DEFAULT NOW(),
            message INT DEFAULT 0,
            reaction INT DEFAULT 0,
            point INT DEFAULT 0,                                                                                                        
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
                        '''}
    copy_table = {'threads_staging': f'''
        COPY {table}
        FROM '{filepath}'
        IAM_ROLE '{iam}'        
        JSON 's3://voz-s3-npt/files/threads/test/jsonpaths.txt'              
        GZIP 
        ACCEPTINVCHARS ' ' 
        TRUNCATECOLUMNS 
        TRIMBLANKS
        ;
    ''', 'participants': f'''
        COPY {table}
        FROM '{filepath}'
        IAM_ROLE '{iam}'                
        IGNOREHEADER 1
        DATEFORMAT 'MON DD, YYYY'
        TIMEFORMAT 'MON DD, YYYY HH:MI:SS AM'        
        DELIMITER '\t'              
        GZIP 
        ACCEPTINVCHARS ' ' 
        TRUNCATECOLUMNS 
        TRIMBLANKS
        ;
    ''', 'users_staging': f'''
        COPY {table}
        FROM '{filepath}'
        IAM_ROLE '{iam}'                
        IGNOREHEADER 1           
        DELIMITER '\t'              
        GZIP 
        ACCEPTINVCHARS ' ' 
        TRUNCATECOLUMNS 
        TRIMBLANKS
        ;
    ''' }
    drop_table = f"DROP TABLE IF EXISTS {table};"
    truncate = f"TRUNCATE TABLE {table};"

    with engine.cursor() as cur:
        if table in ["threads_staging", "users_staging"]:
            cur.execute(drop_table)
        cur.execute(create_table[table])
        cur.execute(copy_table[table])
        engine.commit()
        print(f"Copied file {filepath} to table {table} successfully!")


def load_stage_to_prod_redshift(engine, stage, prod):
    '''Move data from staging table to production table'''

    total_rows_old = 0
    find_table = f'SELECT * FROM svv_table_info where "table"=\'{prod}\''
    cur = engine.cursor()
    is_table_exists = cur.execute(find_table).fetchone()
    if is_table_exists is not None:
        delete_duplicate = {'threads': f'''DELETE FROM {prod} USING {stage} WHERE {prod}.thread_id = {stage}.thread_id''',
                            'users': f'''DELETE FROM {prod} USING {stage} WHERE {prod}.user_id = {stage}.user_id'''
                            }
        total_rows_old = cur.execute(f"SELECT count(*) FROM {prod}").fetchone()[0]
        cur.execute(delete_duplicate[prod])
    else:
        create_table = {'threads': f'''        
                CREATE TABLE IF NOT EXISTS {prod} (    
                thread_id INT NOT NULL DISTKEY,
                title TEXT NOT NULL,
                author_name VARCHAR(100) NOT NULL,
                author_id INT NOT NULL,
                reply_count INT DEFAULT 0,
                view_count INT DEFAULT 0,                
                created_at TIMESTAMP DEFAULT NOW(),                                                                                                                 
                updated_at TIMESTAMP DEFAULT NOW(),
                f INT,
                status SUPER,
                word_count SUPER       
                );
            ''', 'users': f'''
                CREATE TABLE IF NOT EXISTS {prod} (  
                    user_id INT NOT NULL DISTKEY,  
                    username VARCHAR(100) NOT NULL,
                    title VARCHAR(100) NOT NULL,
                    location VARCHAR(100),
                    join_date DATE NOT NULL SORTKEY,
                    last_seen TIMESTAMP DEFAULT NOW(),
                    message INT DEFAULT 0,
                    reaction INT DEFAULT 0,
                    point INT DEFAULT 0,                                                                                                        
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                                '''}
        cur.execute(create_table[prod])

    cur.execute(f"INSERT INTO {prod} (SELECT * FROM {stage});")
    cur.execute(f"DROP TABLE IF EXISTS {stage};")
    total_rows_new = cur.execute(f"SELECT count(*) FROM {prod}").fetchone()[0]
    engine.commit()
    engine.close()
    print(f"Loaded {total_rows_new - total_rows_old} new rows from {stage} to {prod} successfully!")
    print(f"Deleted table {stage} successfully!")
