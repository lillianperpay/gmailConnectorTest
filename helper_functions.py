import hashlib
import logging
try:
    import boto3
except ImportError:
    boto3 = None
import json
import datetime
from datetime import datetime, timedelta
import logging

def create_query(after_days: int, labels: list[str]) -> str:
    """
    Builds a Gmail API query string filtering by label and date.
    
    Args:
        after_days (int): Number of days before today to include messages from.
        labels (list[str]): List of Gmail label names.
    
    Returns:
        str: A valid Gmail query string.
    """
    # Compute the date string in YYYY/MM/DD format
    cutoff_date = datetime.now() - timedelta(days=after_days)
    date_str = cutoff_date.strftime("%Y/%m/%d")
    
    # Escape all labels with backslashes
    quoted_labels = [f'label:\\"{label}\\"' for label in labels]
    
    # Combine labels with OR
    if len(quoted_labels) > 1:
        label_part = "(" + " OR ".join(quoted_labels) + ")"
    elif quoted_labels:
        label_part = quoted_labels[0]
    else:
        label_part = ""  # No labels specified
    
    # Build final query
    query_parts = [f"to:invoices@perpay.com", label_part, f"after:{date_str}"]
    query = " ".join(part for part in query_parts if part)  # Skip empty parts
    
    return query

def create_filename(attachment_id, message_id, filename, metadata_lookup):
    """
    Create the filename (filename string) for the attachment based on the attachment id, message id, filename, and metadata lookup
    the filename is also known as the s3 key
    """
    if message_id not in metadata_lookup:
        logging.error(f"Message ID {message_id} not found in metadata_lookup")
        return f"unknown_{filename}"
    
    vendor = metadata_lookup[message_id]["vendor"]
    date = metadata_lookup[message_id]["date"]

    # change the date so it doesn't have slashes because that creates sub-folders in S3
    date = date.replace("/", "-")

    # Create a unique hash for the attachment
    messageid_attachment_id = f"{message_id}_{attachment_id}"
    hash = hashlib.sha256(messageid_attachment_id.encode("utf-8")).hexdigest()[:4]

    if filename[-3:] == "pdf":
        file_type = "pdf"
    elif filename[-3:] == "csv":
        file_type = "csv"
    else:
        logging.error(f"Unknown file type: {filename}")

    s3_key = f"{vendor}_{date}_{filename[:-3]}_{hash}.{file_type}"
    return s3_key

def extract_attachments_from_payload(payload):
    """
    Extract attachment information from a payload object.
    
    Args:
        payload (Dict[str, Any]): The payload object to search for attachments
        
    Returns:
        List[Dict[str, Any]]: List of attachment information dictionaries
    """
    attachments = []
    
    filename = payload.get('filename', '')
    body = payload.get('body', {})
    attachment_id = body.get('attachmentId')
    
    # Debug logging
    logging.debug(f"Checking payload: filename='{filename}', has_attachment_id={bool(attachment_id)}")
    
    # Check if this is a CSV or PDF attachment
    if attachment_id and filename:
        filename_lower = filename.lower()
        if filename_lower.endswith(('.csv', '.pdf')):
            attachments.append({
                'attachmentId': attachment_id,
                'filename': filename,
            })
            logging.info(f"Found attachment: {filename} (ID: {attachment_id[:20]}...)")
        else:
            logging.debug(f"Skipping non-CSV/PDF file: {filename}")
    elif attachment_id and not filename:
        logging.debug(f"Found attachment ID but no filename: {attachment_id[:20]}...")
    elif filename and not attachment_id:
        logging.debug(f"Found filename but no attachment ID: {filename}")
    
    return attachments

def upload_to_s3(file_name, filedata, bucket_name):
    # Create an STS client
    sts_client = boto3.client('sts')

    # Assume the IAM role
    # try:
    #     assumed_role = sts_client.assume_role(
    #         RoleArn= 'arn:aws:iam:::role/pp-stage-admin-role',
    #         RoleSessionName= 'lillianjiang'
    #     )
    #     credentials = assumed_role['Credentials']
    # except Exception as e:
    #     logging.error(f"Error assuming role: {e}", exc_info=True)
    #     return False

    # # Create a new session with the temporary credentials
    # try:
    #     session = boto3.Session(
    #         aws_access_key_id=credentials['AccessKeyId'],
    #         aws_secret_access_key=credentials['SecretAccessKey'],
    #         aws_session_token=credentials['SessionToken']
    #     )
    # except Exception as e:
    #     logging.error(f"Error creating session: {e}", exc_info=True)
    #     return False

    # Create an S3 client using the new session
    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=f"raw_files/{file_name}", Body=filedata)
        logging.info(f"Successfully uploaded {file_name} to S3 bucket {bucket_name}/raw_files")
        return True
    except Exception as e:
        logging.error(f"Error creating S3 client: {e}", exc_info=True)
        return False

def send_attachments_to_s3(file_data, s3_key, bucket_name):
    """
    This sends a file to S3 based on the attachment data and folder path 
    
    Args:
        file_data: The binary data of the file
        s3_key: The S3 key (path) where the file should be stored
        bucket_name: The S3 bucket name
        
    Returns:
        bool: True if upload was successful, False otherwise
    """
    if boto3 is None:
        logging.error("boto3 not available - cannot upload to S3")
        return False
        
    s3 = boto3.client('s3')
    try:
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=file_data)
        logging.info(f"Successfully uploaded {s3_key} to bucket {bucket_name}")
        return True
    except Exception as e:
        logging.error(f"Error uploading {s3_key} to S3: {e}", exc_info=True)
        return False

def get_or_create_label_id(service, label_name):
    """Finds a label's ID by name. Creates it if it doesn't exist."""
    
    try:
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        
        for label in labels:
            if label['name'] == label_name:
                return label['id'] 
                
    except Exception as e:
        logging.error(f"Error listing labels: {e}", exc_info=True)
        return None

    logging.info(f"Label '{label_name}' not found, creating it...")
    label_body = {
        'name': label_name,
        'labelListVisibility': 'labelShow',
        'messageListVisibility': 'show'
    }
    
    try:
        new_label = service.users().labels().create(userId='me', body=label_body).execute()
        logging.info(f"Created label with ID: {new_label['id']}")
        return new_label['id']
    except Exception as e:
        logging.error(f"Error creating label '{label_name}': {e}", exc_info=True)
        return None

def add_etl_processed_label(service, message_id):
    """
    This adds the ETL-Processed label to the message
    Args:
        service: Gmail service object
        message_id: The ID of the message to add the label to
    Returns:
        bool: True if the label was added successfully, False otherwise
    """ 
    
    label = "ETL-Processed"
    try:
        service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'addLabelIds': [label]}
        ).execute()
        logging.info(f"Successfully added {label} label to message {message_id}")
        return True
    except Exception as e:
        logging.error(f"Error adding {label} label to message {message_id}: {e}", exc_info=True)
        return False

def make_labels_dict(service, makeFile):
    """
    Args:
        service: Gmail service object
        makeFile: boolean to indicate saving off the dictionary into a separate file or not

    This function queries the Gmail API for label information 
    It creates a dictionary where the key is the label name and the value is the label id
    This function is helpful to use when adding new vendors 
    The result of labels_dict will be stored in its own file so we don't always have to call the api 
    """
    labels_response = service.users().labels().list(userId='me').execute()
    labels = labels_response.get('labels', [])
    labels_dict = {label['name']: label['id'] for label in labels}

    if makeFile:
        filename = "labelsDict"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(labels_dict, f, indent=4)
            print(f"Successfully saved labels to {filename}")
        except IOError as e:
            print(f"Error: Could not save file {filename}. {e}")
        except Exception as e:
            print(f"An unexpected error occurred during saving: {e}")

    return labels_dict
