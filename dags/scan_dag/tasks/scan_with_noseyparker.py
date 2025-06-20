import os
import subprocess
from pathlib import Path
from airflow.decorators import task
from airflow.exceptions import AirflowException


@task
def scan_repo_with_noseyparker(repo_url: str, output_path: str = "/opt/airflow/results/scan.json"):
    """
    Scan a repository using noseyparker with proper permission handling

    Args:
        repo_url: Git repository URL to scan
        output_path: Full path for JSON output file
    """
    # Define paths
    datastore_dir = Path("/opt/airflow/scans/datastore.np")
    output_file = Path(output_path)

    try:
        # Scan command
        scan_cmd = [
            "/opt/airflow/noseyparker",
            "scan",
            "--datastore", str(datastore_dir),
            "--git-url", repo_url
        ]

        # Report command
        report_cmd = [
            "/opt/airflow/noseyparker",
            "report",
            "--datastore", str(datastore_dir),
            "--format", "json",
            "-o", str(output_file)
        ]

        # Execute scan
        subprocess.run(scan_cmd, check=True, capture_output=True, text=True)

        # Generate report
        subprocess.run(report_cmd, check=True, capture_output=True, text=True)

        # Set output file permissions
        if output_file.exists():
            os.chmod(output_file, 0o666)

        return str(output_file)

    except subprocess.CalledProcessError as e:
        error_msg = f"Command failed: {e.cmd}\nError: {e.stderr}"
        raise AirflowException(error_msg)
    except Exception as e:
        raise AirflowException(f"Setup failed: {str(e)}")