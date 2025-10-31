"""
This script is used to add a new vendor to the Perpay account.
It will:
1. Find the label ID for the vendor name from Gmail labels
2. Download the vendor mapping and logging file from S3
3. Add the new vendor pair to the mapping and reupload to S3
4. Append the change you made to the logging file and upload to S3

All you need to do is set the vendor_name variable and run the script.
"""

import json
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from helper_functions import get_gmail_service

# S3 Configuration
BUCKET_NAME = "ana-stage-v2-accounts-payable-s3-zy9l1"
VENDOR_MAPPING_S3_KEY = "VendorMapping/vendor_mapping.json"
VENDOR_LOGGING_S3_KEY = "VendorMapping/vendor_logging.json"

def add_perpay_vendor(service, perpay_vendor):
    """
    Find the label ID for a vendor name from Gmail labels.
    
    Args:
        service: Gmail service object
        perpay_vendor: Name of the vendor to find
    
    Returns:
        dict: Dictionary with vendor name as key and label ID as value, or empty dict if not found
    """
    # Make API call to get all the labels 
    labels_response = service.users().labels().list(userId='me').execute()
    labels = labels_response.get('labels', [])

    # Add the new label to the dictionary
    new_dict_pair = {label['id']: label['name'] for label in labels if label['name'] == perpay_vendor}

    if len(new_dict_pair) > 1:
        print(f"ERROR: the {perpay_vendor} is found multiple times in the labels. Please check the invoies@perpay.com account")
        logging.error(f"Vendor '{perpay_vendor}' found multiple times in Gmail labels")
        return None
    
    if not new_dict_pair:
        print(f"ERROR: the new perpay vendor {perpay_vendor} is not in the labels. Please check spelling")
        logging.error(f"Vendor '{perpay_vendor}' not found in Gmail labels")
    
    logging.info(f"Found vendor '{perpay_vendor}' with label ID: {list(new_dict_pair.keys())[0]}")
    
    return new_dict_pair


def download_from_s3(s3_key, bucket_name):
    """
    Download a file from S3.
    
    Args:
        s3_key: The S3 key (path) of the file to download
        bucket_name: The S3 bucket name
    
    Returns:
        bytes: File contents, or None if error or file doesn't exist
    """
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        file_data = response['Body'].read()
        logging.info(f"Successfully downloaded {s3_key} from S3")
        return file_data
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'NoSuchKey':
            logging.warning(f"File {s3_key} does not exist in S3. Will create new file.")
            return None
        else:
            logging.error(f"Error downloading {s3_key} from S3: {e}", exc_info=True)
            return None
    except Exception as e:
        logging.error(f"Error downloading {s3_key} from S3: {e}", exc_info=True)
        return None


def upload_to_s3(s3_key, file_data, bucket_name):
    """
    Upload a file to S3.
    
    Args:
        s3_key: The S3 key (path) where the file should be stored
        file_data: The data to upload (bytes or string)
        bucket_name: The S3 bucket name
    
    Returns:
        bool: True if upload was successful, False otherwise
    """
    try:
        s3 = boto3.client('s3')
        # Convert string to bytes if needed
        if isinstance(file_data, str):
            file_data = file_data.encode('utf-8')
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=file_data)
        logging.info(f"Successfully uploaded {s3_key} to bucket {bucket_name}")
        return True, "uploaded"
    except Exception as e:
        logging.error(f"Error uploading {s3_key} to S3: {e}", exc_info=True)
        return False, "failed to upload"


def download_vendor_mapping(bucket_name):
    """
    Download the vendor mapping file from S3.
    
    Args:
        bucket_name: The S3 bucket name
    
    Returns:
        dict: Vendor mapping dictionary (vendor_name -> label_id), or empty dict if file doesn't exist
    """
    file_data = download_from_s3(VENDOR_MAPPING_S3_KEY, bucket_name)
    if file_data is None:
        logging.info("Vendor mapping file does not exist. Starting with empty mapping.")
        return {}
    
    try:
        vendor_mapping = json.loads(file_data.decode('utf-8'))
        logging.info(f"Downloaded vendor mapping with {len(vendor_mapping)} vendors")
        return vendor_mapping
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing vendor mapping JSON: {e}", exc_info=True)
        return {}


def download_vendor_logging(bucket_name):
    """
    Download the vendor changes logging file from S3.
    
    Args:
        bucket_name: The S3 bucket name
    
    Returns:
        list: List of change log entries, or empty list if file doesn't exist
    """
    file_data = download_from_s3(VENDOR_LOGGING_S3_KEY, bucket_name)
    if file_data is None:
        logging.info("Vendor logging file does not exist. Starting with empty log.")
        return []
    
    try:
        change_log = json.loads(file_data.decode('utf-8'))
        logging.info(f"Downloaded vendor change log with {len(change_log)} entries")
        return change_log
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing vendor logging JSON: {e}", exc_info=True)
        return []


def update_vendor_mapping(vendor_mapping, new_vendor_pair):
    """
    Add a new vendor to the mapping.
    
    Args:
        vendor_mapping: Current vendor mapping dictionary
        new_vendor_pair: Dictionary with vendor name as key and label ID as value
    
    Returns:
        dict: Updated vendor mapping
    """
    vendor_mapping.update(new_vendor_pair)
    logging.info(f"Updated vendor mapping with {new_vendor_pair}")
    return vendor_mapping


def log_vendor_change(change_log, vendor_name, label_id, action="added"):
    """
    Add an entry to the change log.
    
    Args:
        change_log: Current list of change log entries
        vendor_name: Name of the vendor
        label_id: Label ID for the vendor
        action: Action performed (default: "added")
    
    Returns:
        list: Updated change log
    """
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "vendor_name": vendor_name,
        "label_id": label_id,
        "action": action
    }
    change_log.append(log_entry)
    logging.info(f"Added log entry: {log_entry}")
    return change_log


def main():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # TODO: fill out the vendor name 
    vendor_name = "Megagoods"
    
    if not vendor_name:
        print("ERROR: Please set the vendor_name variable before running this script")
        logging.error("vendor_name is empty. Cannot proceed.")
        return
    
    # Step 1: Find the matching label id for the vendor name, ensure that the vendor name exists
    service = get_gmail_service()
    
    new_dict_pair = add_perpay_vendor(service, vendor_name)
    
    # Extract the vendor name and label ID from the dictionary
    label_id = list(new_dict_pair.keys())[0]
    vendor_name_from_dict = new_dict_pair[label_id]
    
    print(f"Found vendor '{vendor_name_from_dict}' with label ID: {label_id}")
    
    # Step 2: Download the vendor mapping and logging file from S3 
    print("Downloading vendor mapping from S3...")
    vendor_mapping = download_vendor_mapping(BUCKET_NAME)
    
    print("Downloading vendor change log from S3...")
    change_log = download_vendor_logging(BUCKET_NAME)
    
    # Step 3: Add the new vendor pair to the mapping and reupload to S3 
    print("Updating vendor mapping...")
    vendor_mapping = update_vendor_mapping(vendor_mapping, new_dict_pair)
    
    # Convert to JSON and upload
    mapping_json = json.dumps(vendor_mapping, indent=4)
    print("Uploading updated vendor mapping to S3...")
    upload_success, action = upload_to_s3(VENDOR_MAPPING_S3_KEY, mapping_json, BUCKET_NAME)

    if upload_success:
        print("Successfully uploaded vendor mapping to S3")
    
    # Step 4: Append the change you made to the logging file and upload to S3 
    print("Logging vendor change...")
    change_log = log_vendor_change(change_log, vendor_name_from_dict, label_id, action)
    
    # Convert to JSON and upload
    logging_json = json.dumps(change_log, indent=4)
    print("Uploading updated change log to S3...")
    log_upload_success = upload_to_s3(VENDOR_LOGGING_S3_KEY, logging_json, BUCKET_NAME)
    
    if not log_upload_success:
        print("ERROR: Failed to upload change log to S3")
        logging.error("Failed to upload change log to S3")
        return
    
    print("Successfully uploaded change log to S3")
    print(f"\n✅ Successfully added vendor '{vendor_name_from_dict}' with label ID '{label_id}'")
    print(f"   - Vendor mapping updated and uploaded to S3")
    print(f"   - Change logged and uploaded to S3")


if __name__ == "__main__":
    main()