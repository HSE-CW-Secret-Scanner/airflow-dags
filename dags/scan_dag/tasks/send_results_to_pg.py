import psycopg2
from psycopg2 import sql
from airflow.decorators import task


@task
def send_results_to_pg(flattened_data, db_config):
    """Upload flattened data to PostgreSQL"""
    conn = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            dbname=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )

        cur = conn.cursor()

        # Prepare the SQL query
        insert_query = sql.SQL("""
            INSERT INTO secrets (
                repository_id, detected_at, commit_hash, file_path, 
                line, code_snippet, author, author_email, 
                secret_hash, secret_type
            ) VALUES (
                (SELECT id FROM repositories WHERE name LIKE %s), 
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING id
        """)

        # For each finding, first ensure repository exists, then insert secret
        for finding in flattened_data:
            # Extract repo name from path (simplified example)
            repo_name = finding['repo_path'].split('/')[-1] if finding['repo_path'] else 'unknown'

            # Insert or get repository
            cur.execute("""
                INSERT INTO repositories (name, last_updated, last_scanned)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET last_scanned = EXCLUDED.last_scanned
                RETURNING id
            """, (repo_name, finding['detected_at'], finding['detected_at']))

            repo_id = cur.fetchone()[0]

            # Insert secret
            cur.execute(insert_query, (
                f'%{repo_name}%',  # For the LIKE clause
                finding['detected_at'],
                finding['commit_hash'],
                finding['file_path'],
                finding['line'],
                finding['snippet'],
                finding['author'],
                finding['author_email'],
                finding['secret'],  # Using finding_id as secret_hash
                finding['rule_name']  # Using rule_name as secret_type
            ))

            secret_id = cur.fetchone()[0]

            # Optionally add status change
            cur.execute("""
                INSERT INTO secret_status_changes (secret_id, status, marked_at)
                VALUES (%s, 'detected', %s)
            """, (secret_id, finding['detected_at']))

        conn.commit()
        print(f"Successfully inserted {len(flattened_data)} findings")

    except Exception as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
