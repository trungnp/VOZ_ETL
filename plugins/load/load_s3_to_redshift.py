# params = config(filename="/Users/trungnp/Library/Mobile Documents/com~apple~CloudDocs/Study/DE/voz/plugins/utilities/conf.ini", section="s3")
# S3_SECKET_KEY = params['AWS_SECRET_KEY']
# S3_ACCESS_KEY = params['AWS_ACCESS_KEY_ID']
# S3_BUCKET = params['BUCKET_NAME']

def load_s3_to_redshift(engine, filepath, iam, table):
    create_table = {'participants': f'''
        CREATE TABLE IF NOT EXISTS {table} (
            thread_id INT DISTKEY SORTKEY,
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
        --DATEFORMAT 'MON DD, YYYY'
        --TIMEFORMAT 'MON DD, YYYY HH:MI:SS AM'        
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
        if "staging" in table:
            cur.execute(drop_table)
        cur.execute(create_table[table])
        cur.execute(copy_table[table])
        engine.commit()
        print(f"Copied file {filepath} to table {table} successfully!")
    # with engine.begin() as conn:
    # engine.execute(create_table_threads)
    # engine.execute(copy_query)


def load_stage_to_prod_redshift(engine, stage, prod):
    total_rows_old = 0
    find_table = f'SELECT * FROM svv_table_info where "table"=\'{prod}\''
    cur = engine.cursor()
    is_table_exists = cur.execute(find_table).fetchone()
    if is_table_exists is not None:
        # with engine.cursor() as cur:
        delete_duplicate = {'threads': f'''DELETE FROM {prod} USING {stage} WHERE {prod}.thread_id = {stage}.thread_id''',
                            'users': f'''DELETE FROM {prod} USING {stage} WHERE {prod}.user_id = {stage}.user_id'''
                            }
        total_rows_old = cur.execute(f"SELECT count(*) FROM {prod}").fetchone()[0]
        # delete_duplicate_thread = f'''DELETE FROM {prod} USING {stage}
        #                               WHERE {prod}.thread_id = {stage}.thread_id
        #                               '''
        # total_rows_old = cur.execute(f"SELECT count(*) FROM {prod}").fetchone()[0]
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

    # with engine.begin() as conn:
    cur.execute(f"INSERT INTO {prod} (SELECT * FROM {stage});")
    cur.execute(f"DROP TABLE IF EXISTS {stage};")
    total_rows_new = cur.execute(f"SELECT count(*) FROM {prod}").fetchone()[0]
    print(f"Loaded {total_rows_new - total_rows_old} new rows from {stage} to {prod} successfully!")
    print(f"Deleted table {stage} successfully!")
    engine.commit()
    engine.close()

# def load_object_to_redshift(pg_engine, rs_conn, table, iam):
#     latest_update_date = pg_engine.execute("SELECT * FROM latest_update_date").fetchone()
#     if latest_update_date:
#         query = f"SELECT * FROM {table} WHERE updated_at >= '{latest_update_date[0]}'"
#     else:
#         query = f"SELECT * from {table}"
#     new_latest_update = pg_engine.execute("SELECT MAX(updated_at) FROM threads;").fetchone()[0]
#     df = pd.read_sql(query, pg_engine)
#     try:
#         if table == "threads":
#             replace_M_K(df)
#             df = df.assign(word_count=lambda x: count_word(x['participants']))
#             df['created_at'] = df['created_at'].astype(str)
#             df['updated_at'] = df['updated_at'].astype(str)
#             cols = df.columns.tolist()
#             new_cols = cols[:6] + cols[7:] + cols[6:7]
#             data_json = df[new_cols].to_json(orient="records")
#             threads_json = data_json[1:-1].replace(',{"thread_id":', '{"thread_id":')
#             filepath_threads = f'files/redshift/threads/{new_latest_update.strftime("%Y%m%d%H%M%S")}.json.gz'
#             upload_to_s3(threads_json, filepath=filepath_threads, BUCKET_NAME=S3_BUCKET, AWS_SECRET_KEY=S3_SECKET_KEY,
#                          AWS_ACCESS_KEY_ID=S3_ACCESS_KEY)
#             load_s3_to_redshift(rs_conn, f's3://{S3_BUCKET}/{filepath_threads}', iam,
#                                 "threads_staging")
#             load_stage_to_prod_redshift(rs_conn, "threads_staging", "threads")
#         elif table == "participants":
#             # create new DF from column participants
#             participants = df[~df['participants'].isna()]
#             participants_df = None
#             for i in range(participants.shape[0]):
#                 df_par = pd.DataFrame(participants['participants'].iloc[i])
#                 df_par['thread_id'] = participants['thread_id'].iloc[i]
#                 df_par = df_par[['thread_id', 'user_id', 'username', 'reply', 'created_at', 'love', 'brick']]
#                 participants_df = df_par if participants_df is None else pd.concat([participants_df, df_par])
#             # result['created_at'] = result['created_at'].astype(str)
#             participants_df.drop_duplicates(subset=['thread_id', 'user_id', 'created_at'], keep="last", inplace=True)
#             filepath_participants = f'files/redshift/participants/{new_latest_update.strftime("%Y%m%d%H%M%S")}.csv.gz'
#             upload_to_s3(participants_df, filepath=filepath_participants, BUCKET_NAME=S3_BUCKET,
#                          AWS_SECRET_KEY=S3_SECKET_KEY, AWS_ACCESS_KEY_ID=S3_ACCESS_KEY)
#             load_s3_to_redshift(rs_conn, f's3://{S3_BUCKET}/{filepath_participants}', iam,
#                                 "participants")
#         else:
#             return f"Table {table} doesnt exists!"
#         pg_engine.execute(f"TRUNCATE latest_update_date; INSERT INTO latest_update_date VALUES('{new_latest_update}');")
#     except Exception as E:
#         print(E)