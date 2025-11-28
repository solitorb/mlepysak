#!/usr/bin/env python3

"""
sql2csv   - SQL to CSV converter
Copyright - (C) 2025 Rudra / VeeraBhadraReddy / cosmoid / Andy Freeman
License   - MIT
"""

import argparse
import csv
import logging
import os
from importlib.metadata import distribution, version
from typing import Dict, Optional

import boto3
import psycopg2
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Configure logging with proper formatting
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Add logging for successful database connections
def log_database_connection_success(host: str, port: int, database: str):
    logger.info(
        f"Successfully connected to PostgreSQL database at {host}:{port}/{database}"
    )


# Add logging for successful S3 uploads
def log_s3_upload_success(file_name: str):
    logger.info(
        f"Successfully uploaded {file_name} to S3 bucket {os.getenv('S3_BUCKET_NAME')}"
    )


def run_sql_script(
    file_path: str, db_credentials: Dict[str, str], ca_cert_path: Optional[str] = None
) -> str:
    """
    Executes an SQL script using the provided credentials and returns the output.

    This function connects to a PostgreSQL database using psycopg2 and executes
    the SQL script from the provided file path.

    Args:
        file_path (str): Path to the SQL script file.
        db_credentials (dict): Database credentials containing host, user, password, and database name.
        ca_cert_path (str, optional): Path to PostgreSQL CA certificate file for SSL connection.

    Returns:
        str: Output of the SQL script execution.

    Raises:
        psycopg2.Error: If database connection or query execution fails.
    """
    logger.info(f"  Executing SQL script: {file_path}")
    try:
        # Connect to PostgreSQL database
        logger.info(
            f"    Connecting to database at {db_credentials['host']}:{db_credentials['port']}"
        )

        # Set SSL mode based on environment variable or default to 'require'
        ssl_mode = os.getenv("PGSSLMODE", "require")
        connection_params = {
            "host": db_credentials["host"],
            "user": db_credentials["user"],
            "password": db_credentials["password"],
            "database": db_credentials["database"],
            "port": db_credentials["port"],
            "sslmode": ssl_mode,
        }

        # Add sslrootcert parameter if CA certificate is provided
        if ca_cert_path:
            connection_params["sslrootcert"] = ca_cert_path

        connection = psycopg2.connect(**connection_params)

        # Log successful connection
        log_database_connection_success(
            db_credentials["host"], db_credentials["port"], db_credentials["database"]
        )

        # Create a cursor to execute queries
        cursor = connection.cursor()

        # Read the SQL script file
        logger.debug("    Reading SQL script file")
        with open(file_path, "r") as sql_file:
            sql_script = sql_file.read()

        # Execute the SQL script
        logger.debug("    Executing SQL script")
        cursor.execute(sql_script)

        # If the script returns results, fetch them
        if cursor.description:  # Check if there are results to fetch
            rows = cursor.fetchall()
            # Get column names
            column_names = [desc[0] for desc in cursor.description]

            # Prepare output as string
            output = ""
            # Add column headers
            output += ",".join(column_names) + "\n"
            # Add data rows
            for row in rows:
                output += ",".join(str(val) for val in row) + "\n"
        else:
            # If no results (like INSERT, UPDATE, DELETE), return success message
            output = "Query executed successfully\n"

        # Close cursor and connection
        cursor.close()
        connection.close()

        logger.info("  SQL script executed successfully")
        return output
    except psycopg2.Error as e:
        # Print an error message if the SQL script execution fails
        logger.error(f"Error running SQL script {file_path}: {e}")
        raise


def upload_to_s3(file_name: str, file_path: str) -> None:
    """
    Uploads a file to an S3 bucket using AWS credentials from environment variables.

    This function creates an S3 client using the AWS access key and secret key
    retrieved from environment variables. It then uploads the specified file to
    the S3 bucket identified by the S3_BUCKET_NAME environment variable.

    Args:
        file_name (str): Name of the file to upload.
        output_dir (str): Directory containing the file to upload.

    Raises:
        Exception: If the upload to S3 fails.
    """
    logger.info(f"  Uploading file to S3: {file_name}")
    try:
        # Retrieve AWS credentials from environment variables
        # These credentials are typically set using a .env file or exported environment variables
        # On very new attempt, the bucket does not exist
        bucket_exists: bool = False
        do_s3_bucket = os.getenv("S3_BUCKET_NAME")
        region = os.getenv("DO_REGION_NAME")

        s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=os.getenv("DO_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("DO_SECRET_ACCESS_KEY"),
            endpoint_url=os.getenv("DO_ENDPOINT_URL"),
        )
        # Construct the full path to the file to be uploaded
        # file_path = os.path.join(output_dir, file_name)
        # Upload the file to the specified S3 bucket with public-read ACL
        logger.info(
            f"    Uploading {file_path} to S3 bucket {os.getenv('S3_BUCKET_NAME')}"
        )

        # See if bucket exists
        try:
            logger.info(f"    Checking to see if {do_s3_bucket} S3 bucket exists")
            s3.head_bucket(Bucket=do_s3_bucket)
            bucket_exists = True
            logger.info(f"Bucket {do_s3_bucket} exists.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.info(f"Bucket {do_s3_bucket} does not exist. Creating it")
                s3.create_bucket(Bucket=do_s3_bucket)
                bucket_exists = True
            else:
                logger.error(f"Error checking bucket: {e}")
                raise

        # If bucket exists
        # Disable Current ACL Perms
        if bucket_exists:
            # Disable public access blocks (allows public ACLs)
            s3.put_public_access_block(
                Bucket=do_s3_bucket,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": False,
                    "IgnorePublicAcls": False,
                    "BlockPublicPolicy": False,
                    "RestrictPublicBuckets": False,
                },
            )
            logger.info(
                f"Public access block settings disabled for bucket {do_s3_bucket}"
            )

            # Set ownership to ObjectWriter (uploader sets ACLs)
            s3.put_bucket_ownership_controls(
                Bucket=do_s3_bucket,
                OwnershipControls={
                    "Rules": [{"ObjectOwnership": "ObjectWriter"}],
                },
            )
            logger.info(f"Ownership controls set for bucket {do_s3_bucket}")

            # Set bucket ACL to public-read
            s3.put_bucket_acl(Bucket=do_s3_bucket, ACL="public-read")
            logger.info(f"Bucket ACL set to 'public-read' for {do_s3_bucket}")

            # Set to only public-read
            s3.upload_file(
                file_path,
                os.getenv("S3_BUCKET_NAME"),
                file_name,
                ExtraArgs={"ACL": "public-read"},
            )

            # Log successful upload
            log_s3_upload_success(file_name)
    except Exception as e:
        # Print an error message if the upload fails
        logger.error(f"Error uploading {file_name} to S3: {e}")
        raise


def parse_pgpass_file(pgpass_file_path: Optional[str]) -> Dict[str, str]:
    """
    Parse a PostgreSQL .pgpass file and return credentials.

    The .pgpass file format is: hostname:port:database:username:password
    This function reads the file and returns the first matching entry.

    Args:
        pgpass_file_path (str): Path to the .pgpass file

    Returns:
        dict: Database credentials (host, port, user, password, database)
    """
    # Default values
    credentials = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", 5432)),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "database": os.getenv("PGDATABASE"),
    }

    # If no pgpass file specified, return default credentials
    if not pgpass_file_path:
        return credentials

    try:
        # Read the .pgpass file
        with open(pgpass_file_path, "r") as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith("#"):
                    continue

                # Parse the line: hostname:port:database:username:password
                parts = line.split(":")
                if len(parts) >= 5:
                    # For simplicity, we'll use the first matching entry
                    # In a real implementation, you might want to match by hostname
                    credentials = {
                        "host": parts[0],
                        "port": int(parts[1]) if parts[1] else 5432,
                        "database": parts[2],
                        "user": parts[3],
                        "password": parts[4],
                    }
                    break
    except Exception as e:
        logger.error(f"Error reading .pgpass file {pgpass_file_path}: {e}")
        # Return default credentials if file reading fails
        pass

    return credentials


def generate_html_index(args, csv_files, base_url):
    """
    Generate a 1-column HTML index file with 'Datasets' header and CSV filenames as rows.

    Args:
        args: Command line arguments
        csv_files: List of CSV file names processed
    """
    # Create the HTML content for a 1-column table
    html_content = """<!DOCTYPE html>
<html>
<head>
    <title>CSV Files Index</title>
    <meta charset="UTF-8">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <h1>CSV Files Index</h1>
    <table>
        <thead>
            <tr>
                <th>Datasets</th>
            </tr>
        </thead>
        <tbody>
"""

    # Add rows for each CSV file with hyperlinks
    for csv_file in csv_files:

        # Create the full URL
        full_url = f"https://{base_url}/{csv_file}"

        # Add table row with hyperlink
        html_content += f"            <tr>\n"
        html_content += f'                <td><a href="{full_url}", target="_blank">{csv_file}</a></td>\n'
        html_content += f"            </tr>\n"

    # Close the HTML
    html_content += """        </tbody>
    </table>
</body>
</html>"""

    return html_content


def process_sql_file(file_path: str, args, output_dir: str, input_sql_dir: str) -> None:
    """
    Process a single SQL file and convert it to CSV format.

    Args:
        file_path (str): Path to the SQL file
        args: Command line arguments
        output_dir (str): Output directory for CSV files
        input_sql_dir (str): Input directory containing SQL files
    """
    logger.info(f"  Processing file: {os.path.basename(file_path)}")
    try:
        # Get database credentials
        db_credentials = parse_pgpass_file(args.pgsql_creds_file)

        # Execute the SQL script and capture the output
        output = run_sql_script(
            file_path,
            db_credentials,
            args.pgsql_ca_crt,
        )

        # Generate CSV file name by replacing .sql with .csv
        file_name = os.path.basename(file_path)
        csv_file_name = file_name.replace(".sql", ".csv")

        # Determine CSV file path based on recursive behavior
        if not args.no_recursive:
            # Preserve directory structure in output for recursive behavior
            relative_path = os.path.relpath(os.path.dirname(file_path), input_sql_dir)
            if relative_path == ".":
                csv_file_path = os.path.join(output_dir, csv_file_name)
            else:
                csv_file_path = os.path.join(output_dir, relative_path, csv_file_name)
        else:
            # Non-recursive: no directory structure preservation
            csv_file_path = os.path.join(output_dir, csv_file_name)

        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

        # Write the SQL output to a CSV file
        logger.debug(f"  Writing CSV output to: {csv_file_path}")
        with open(csv_file_path, "w", newline="") as f:
            writer = csv.writer(f)
            for line in output.splitlines():
                writer.writerow([line])

        # Upload the CSV file to S3
        upload_to_s3(csv_file_name, csv_file_path)
        logger.info(f"  File {file_name} processed successfully")
    except Exception as e:
        # Print an error message if processing the file fails
        logger.error(f"Error processing file {file_path}: {e}")
        raise


def main() -> None:
    """
    Main function to process SQL files and upload them to S3.

    This function parses command-line arguments, processes each SQL file in the input directory,
    converts the output to CSV format, and uploads the CSV files to S3. It also handles
    optional credential files and version information.

    Args:
        None (uses command-line arguments and environment variables)
    """
    # Set up argument parser for command-line options
    parser = argparse.ArgumentParser(
        description="Process SQL scripts, run queries and store resutls in CSV format and upload them to S3."
    )

    # Required arguments
    parser.add_argument("--input-sql-dir", help="Directory containing .sql files")
    parser.add_argument("--output-dir", help="Output directory for CSV files")
    parser.add_argument(
        "--pgsql-creds-file",
        help="Path to .pgsql credentials file",
    )
    parser.add_argument(
        "--aws-creds-file",
        help="Path to AWS credentials .env file",
    )

    # Optional arguments group
    optional_args = parser.add_argument_group("optional arguments")

    optional_args.add_argument(
        "--no-recursive",
        action="store_true",
        help="Do not search input-sql-dir recursively for .sql files (default: False)",
        default=False,
    )
    optional_args.add_argument(
        "--pgsql-ca-crt",
        help="Path to PostgreSQL CA certificate file for SSL connection",
    )
    optional_args.add_argument(
        "--html-loc",
        default="sprocket-public-datasets.nyc3.cdn.digitaloceanspaces.com/datasets",
        required=False,
        help="Base URL for constructing dataset links.",
    )
    optional_args.add_argument(
        "--version",
        action="store_true",
        help="Show version information",
    )

    args = parser.parse_args()

    # Get package metadata from pyproject.toml for version and author information
    dist = distribution("mlepysak")
    package_version = version("mlepysak")
    package_author = dist.metadata["Author"]

    # Display version information if requested
    if args.version:
        print(f"\n{dist.name} v{package_version}\nWritten by {package_author}.")
        exit(0)

    logger.info("Starting SQL to CSV conversion process")
    # Validate required arguments
    if not args.input_sql_dir or not args.output_dir:
        parser.error(
            "--input-sql-dir and --output-dir are required unless --version is specified"
        )

    # Load environment variables from .env file or specified credential files
    load_dotenv()

    # Load optional .pgsql credentials file if specified
    if args.pgsql_creds_file:
        load_dotenv(dotenv_path=args.pgsql_creds_file)

    # Load optional AWS credentials file if specified
    if args.aws_creds_file:
        load_dotenv(dotenv_path=args.aws_creds_file)

    # Process each SQL file in the input directory
    logger.info(f"Processing SQL files in directory: {args.input_sql_dir}")

    # Keep track of processed CSV files for index generation
    csv_files = []

    def process_sql_files(args):
        """Helper function to process SQL files with recursive or non-recursive behavior."""
        if not args.no_recursive:
            # Recursively search for .sql files (when --no-recursive is NOT specified)
            for root, dirs, files in os.walk(args.input_sql_dir):
                for file in files:
                    if file.endswith(".sql"):
                        file_path = os.path.join(root, file)
                        # Process the file and collect CSV file name
                        process_sql_file(
                            file_path, args, args.output_dir, args.input_sql_dir
                        )
                        # Extract CSV file name and add to list
                        file_name = os.path.basename(file_path)
                        csv_file_name = file_name.replace(".sql", ".csv")
                        csv_files.append(csv_file_name)
        else:
            # Non-recursive behavior (when --no-recursive is specified)
            for file in os.listdir(args.input_sql_dir):
                if file.endswith(".sql"):
                    file_path = os.path.join(args.input_sql_dir, file)
                    # Process the file and collect CSV file name
                    process_sql_file(
                        file_path, args, args.output_dir, args.input_sql_dir
                    )
                    # Extract CSV file name and add to list
                    file_name = os.path.basename(file_path)
                    csv_file_name = file_name.replace(".sql", ".csv")
                    csv_files.append(csv_file_name)

    process_sql_files(args)

    # Generate and upload HTML index file
    if csv_files:
        logger.info("Generating HTML index file")
        html_content = generate_html_index(args, csv_files, args.html_loc)

        # Write HTML index to a file
        html_file_path = os.path.join(args.output_dir, "index.html")
        with open(html_file_path, "w") as f:
            f.write(html_content)

        # Upload the HTML index file to S3
        upload_to_s3("index.html", html_file_path)

    logger.info("SQL to CSV conversion process completed")


if __name__ == "__main__":
    main()
