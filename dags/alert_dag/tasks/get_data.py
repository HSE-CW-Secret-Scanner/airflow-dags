from datetime import datetime, timedelta
from sqlalchemy import func, and_
from sqlalchemy.exc import SQLAlchemyError
from airflow.decorators import task

from config.data_sources.secrets_db import SecretDatabase


@task
def get_data():
    return [generate_security_report(), generate_alert_message()]

def generate_security_report():
    """Generate security report message with total secrets and false-positives"""
    dpc = SecretDatabase()
    try:
        dpc.connect()
        table_names = ['secrets', 'secret_status_changes']
        dpc.reflect_tables(table_names)

        Secret = dpc.get_table_class('secrets')
        SecretStatusChange = dpc.get_table_class('secret_status_changes')

        session = dpc.Session()

        # Get total number of secrets
        secrets_num = session.query(Secret.id).count()

        # Subquery to find the latest status for each secret
        latest_status_subq = session.query(
            SecretStatusChange.secret_id,
            func.max(SecretStatusChange.marked_at).label('max_marked_at')
        ).group_by(SecretStatusChange.secret_id).subquery()

        # Query to get current false-positives
        fp_num = session.query(SecretStatusChange).join(
            latest_status_subq,
            and_(
                SecretStatusChange.secret_id == latest_status_subq.c.secret_id,
                SecretStatusChange.marked_at == latest_status_subq.c.max_marked_at
            )
        ).filter(SecretStatusChange.status == 'false_positive').count()

        return f"""📝**Security report**📝
Secrets: {secrets_num}
False-positives: {fp_num}"""

    except SQLAlchemyError as e:
        return f"Database error: {e}"
    finally:
        if 'session' in locals():
            session.close()
        dpc.close()


def generate_alert_message(hours=24):
    """Generate alert message with new secrets count and affected repositories"""
    dpc = SecretDatabase()
    try:
        dpc.connect()
        table_names = ['secrets', 'repositories']
        dpc.reflect_tables(table_names)

        Secret = dpc.get_table_class('secrets')
        Repository = dpc.get_table_class('repositories')

        session = dpc.Session()
        time_threshold = datetime.utcnow() - timedelta(hours=hours)

        # Get new secrets count
        new_secrets_num = session.query(Secret.id).filter(
            Secret.detected_at >= time_threshold
        ).count()

        # Get number of affected repositories
        repo_num = session.query(Repository.id).join(
            Secret, Repository.id == Secret.repository_id
        ).filter(
            Secret.detected_at >= time_threshold
        ).distinct().count()

        return f"""⚠**Alert**⚠
Found {new_secrets_num} new potential secrets in {repo_num} repository.
Visit corporate Grafana to check it out"""

    except SQLAlchemyError as e:
        return f"Database error: {e}"
    finally:
        if 'session' in locals():
            session.close()
        dpc.close()