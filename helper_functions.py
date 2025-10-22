import hashlib
import logging
try:
    import boto3
except ImportError:
    boto3 = None

def create_s3_key(attachment_id, message_id, filename, metadata_lookup):
    if message_id not in metadata_lookup:
        logging.error(f"Message ID {message_id} not found in metadata_lookup")
        return f"unknown_{filename}"
    
    vendor = metadata_lookup[message_id]["vendor"]
    date = metadata_lookup[message_id]["date"]

    # Create a unique hash for the attachment
    messageid_attachment_id = f"{message_id}_{attachment_id}"
    hash = hashlib.sha256(messageid_attachment_id.encode("utf-8")).hexdigest()[:4]

    s3_key = f"{vendor}_{date}_{filename}_{hash}"
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

import logging

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