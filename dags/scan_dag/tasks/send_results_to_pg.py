from airflow.decorators import task
from sqlalchemy.exc import SQLAlchemyError

from config.data_sources.secrets_db import SecretDatabase

@task
def send_results_to_pg(flattened_data):
    """Upload flattened data to PostgreSQL"""

    db = None
    try:
        db = SecretDatabase()
        db.connect()
        table_names = ['repositories', 'secrets', 'secret_status_changes']
        db.reflect_tables(table_names)

        Repository = db.get_table_class('repositories')
        Secret = db.get_table_class('secrets')
        SecretStatusChange = db.get_table_class('secret_status_changes')

        session = db.Session()
        count = 0

        for finding in flattened_data:
            repo_name = finding['repo_path'].split('/')[-1] if finding['repo_path'] else 'unknown'

            # Upsert repository
            repo = session.query(Repository).filter_by(name=repo_name).first()
            if repo:
                repo.last_scanned = finding['detected_at']  # Update scan time
            else:
                repo = Repository(
                    name=repo_name,
                    last_updated=finding['detected_at'],
                    last_scanned=finding['detected_at']
                )
                session.add(repo)

            session.flush()  # Get repo ID

            # Create secret record
            secret = Secret(
                repository_id=repo.id,
                detected_at=finding['detected_at'],
                commit_hash=finding['commit_hash'],
                file_path=finding['file_path'],
                line=finding['line'],
                code_snippet=finding['snippet'],
                author=finding['author'],
                author_email=finding['author_email'],
                secret_hash=finding['secret'],
                secret_type=finding['rule_name']
            )
            session.add(secret)
            session.flush()  # Get secret ID

            # Create status record
            status = SecretStatusChange(
                secret_id=secret.id,
                status='detected',
                marked_at=finding['detected_at']
            )
            session.add(status)
            count += 1

        session.commit()
        print(f"Successfully inserted {count} findings")

    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        if 'session' in locals():
            session.rollback()
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if 'session' in locals():
            session.close()
        if db:
            db.close()