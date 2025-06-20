import json
from datetime import datetime
from airflow.decorators import task


@task
def process_data(input_data):
    """Convert nested findings data into flattened structure"""
    flattened = []
    with open(input_data) as f:
        input_data = json.load(f)

    for finding in input_data:
        for match in finding.get('matches', []):
            # Extract provenance data (first git_repo entry)
            provenance = next(
                (p for p in match['provenance'] if p['kind'] == 'git_repo'),
                {}
            )
            commit_metadata = provenance.get('first_commit', {}).get('commit_metadata', {})

            flattened.append({
                "repo_path": provenance.get('repo_path', ''),
                "commit_hash": commit_metadata.get('commit_id', ''),
                "author": commit_metadata.get('author_name', ''),
                "author_email": commit_metadata.get('author_email', ''),
                "rule_name": match.get('rule_name', ''),
                "secret": finding.get('finding_id', ''),
                "snippet": match.get('snippet', {}).get('matching', ''),
                "file_path": provenance.get('first_commit', {}).get('blob_path', ''),
                "line": match.get('location', {}).get('source_span', {}).get('start', {}).get('line', 0),
                "detected_at": datetime.now().isoformat()
            })

    return flattened