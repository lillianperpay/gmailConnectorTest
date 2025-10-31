import logging 
import email.utils
import base64
import time
from typing import List, Dict, Any
from helper_functions import *
import os
import datetime
from datetime import datetime, timedelta


def fetch_message_ids(service, query):
    """
    Get all the message_ids. Handles pagination with nextPageToken.
    """
    try:
        response = service.users().messages().list(
            userId='me',
            maxResults=100,
            q=query,
            includeSpamTrash=False
        ).execute()

        logging.debug(f"First page response: {response}")
        messages = response.get('messages', [])

        while 'nextPageToken' in response:
            response = service.users().messages().list(
                userId='me',
                q=query,
                includeSpamTrash=False,
                pageToken=response['nextPageToken']
            ).execute()
            messages.extend(response.get('messages', []))

        message_ids = [msg['id'] for msg in messages]
        logging.info(f"Found {len(message_ids)} message IDs to check.")
        return message_ids

    except Exception as e:
        logging.error(f"Error fetching message list: {e}")
        return []

def get_messages_metadata_batch(service, message_ids, batch_size=20, delay_between_batches=0.5):
    """
    Fetch metadata for all message_ids in smaller batches to avoid rate limits.
    
    Args:
        service: Gmail service object
        message_ids: List of message IDs to fetch metadata for
        batch_size: Number of messages to process in each batch (default: 20)
        delay_between_batches: Seconds to wait between batches (default: 0.5)
    """
    message_metadata_map = {}
    
    if not message_ids:
        logging.info("No message IDs to batch.")
        return {}
    
    # Process messages in smaller batches to avoid rate limits
    total_batches = (len(message_ids) + batch_size - 1) // batch_size
    logging.info(f"Processing {len(message_ids)} messages in {total_batches} batches of {batch_size}")
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(message_ids))
        batch_message_ids = message_ids[start_idx:end_idx]
        
        logging.info(f"Processing metadata batch {batch_num + 1}/{total_batches} ({len(batch_message_ids)} messages)")
        
        # Process this batch
        batch_results = _process_metadata_batch(service, batch_message_ids)
        message_metadata_map.update(batch_results)
        
        # Add delay between batches to respect rate limits
        if batch_num < total_batches - 1:  # Don't delay after the last batch
            logging.debug(f"Waiting {delay_between_batches} seconds before next batch...")
            time.sleep(delay_between_batches)
    
    logging.info(f"Metadata batch processing complete. Received metadata for {len(message_metadata_map)} messages.")
    return message_metadata_map


def _process_metadata_batch(service, message_ids: List[str]) -> Dict[str, Any]:
    """
    Process a single batch of message IDs for metadata.
    
    Args:
        service: Gmail service object
        message_ids: List of message IDs to process
        
    Returns:
        Dictionary of message_id -> metadata
    """
    message_metadata_map = {}
    
    def metadata_callback(request_id, response, exception):
        """
        This function is called *once for each request* in the batch.
        request_id is the unique ID we gave it (e.g., 'msg-id-A')
        """
        if exception:
            # Handle error for this specific message
            logging.error(f"Failed to get metadata for {request_id}: {exception}")
            if "too many concurrent requests" in str(exception).lower():
                logging.warning(f"Rate limit hit for message {request_id}. Consider reducing batch size.")
        else:
            message_metadata_map[request_id] = response
    
    batch = service.new_batch_http_request(callback=metadata_callback)
    
    # Add each request to the batch
    for msg_id in message_ids:
        request = service.users().messages().get(
            userId='me',
            id=msg_id,
            format='metadata'  
        )
        # We use the msg_id as the 'request_id' so we can map it in the callback
        batch.add(request, request_id=msg_id)
    
    try:
        batch.execute()
        logging.debug(f"Successfully processed metadata batch of {len(message_ids)} messages")
    except Exception as e:
        logging.error(f"Metadata batch request failed: {e}")
        if "quota" in str(e).lower() or "rate" in str(e).lower():
            logging.warning("Rate limit or quota exceeded. Consider increasing delay between batches.")
    
    return message_metadata_map

def create_metadata_lookup(message_metadata_map):
    """
    Create a metadata_lookup dictionary from the metadata_output.json file.
    Only includes emails without the "ETL-Processed" label.
    
    Args:
        metadata_file_path (str): Path to the metadata_output.json file
        
    Returns:
        Dict[str, Dict[str, Any]]: Dictionary with message_id as key and vendor/date info as value
    """
    
    metadata_lookup = {}
    
    for message_id, message_data in message_metadata_map.items():
        try:
            # Check if the email is in our list of labels
            # TODO: uncomment this later 
            label_ids = message_data.get('labelIds', [])
            # if 'ETL-Processed' in label_ids:
            #     logging.debug(f"Skipping message {message_id} - already ETL processed")
            #     continue
            
            # Extract vendor and date information
            vendor = None
            date = None
            
            # Extract headers
            headers = message_data.get('payload', {}).get('headers', [])
            if headers:
                headers_map = {h.get('name', '').lower(): h.get('value', '') for h in headers}
                
                # Extract vendor from delivered-to email
                delivered_to = headers_map.get('delivered-to') or headers_map.get('to')
                if delivered_to:
                    try:
                        _, email_addr = email.utils.parseaddr(delivered_to)
                        logging.debug(f"Parsed email address: {email_addr}")
                        
                        if email_addr and "@" in email_addr:
                            local_part = email_addr.split("@")[0]
                            if "+" in local_part:
                                vendor = local_part.split("+", 1)[1]
                                logging.info(f"Extracted vendor '{vendor}' from email {email_addr}")
                            else:
                                logging.debug(f"No '+' found in email local part: {local_part}")
                        else:
                            # Find the vendor from the label_id_to_check 
                            label_id_to_check = [id for id in label_ids if id.startswith("Label_")]
                            get_vendor_from_label_id(label_id_to_check)
                            continue
                    except Exception as e:
                        logging.error(f"Error parsing email address '{delivered_to}': {e}")
                else:
                    logging.warning(f"Message {message_id} has no 'delivered-to' or 'to' header")
                
                raw_date = headers_map.get("date")                
                if raw_date:
                    try:
                        date_object = email.utils.parsedate_to_datetime(raw_date)
                        date = date_object.strftime("%m/%d/%Y")
                        logging.debug(f"Parsed date '{raw_date}' to timestamp {date}")
                    except Exception as e:
                        logging.error(f"Failed to parse date '{raw_date}': {e}")
            # Only add to lookup if we have both vendor and date

            if vendor and date is not None:
                metadata_lookup[message_id] = {
                    "vendor": vendor,
                    "date": date
                }
                logging.debug(f"Added message {message_id} to metadata_lookup: vendor={vendor}, date={date}")
            else:
                logging.debug(f"Skipping message {message_id} - missing vendor or date: vendor={vendor}, date={date}")
                
        except Exception as e:
            logging.error(f"Error processing message {message_id}: {e}")
            continue
    
    logging.info(f"Created metadata_lookup with {len(metadata_lookup)} emails (excluding ETL-Processed)")

    return metadata_lookup

def get_messages_full_batch(service, metadata_lookup, batch_size=50, delay_between_batches=1.0):
    """
    Get FULL PAYLOAD for the *filtered* list of message_ids in smaller batches to avoid rate limits.
    This will include attachment info
    
    Args:
        service: Gmail service object
        metadata_lookup: Dictionary of message_id -> metadata
        batch_size: Number of messages to process in each batch (default: 50)
        delay_between_batches: Seconds to wait between batches (default: 1.0)
    """
    message_full_payload_map = {}
    message_ids = list(metadata_lookup.keys())
    
    if not message_ids:
        logging.info("No message IDs to fetch full payload for.")
        return {}
    
    # Process messages in smaller batches to avoid rate limits
    total_batches = (len(message_ids) + batch_size - 1) // batch_size
    logging.info(f"Processing {len(message_ids)} messages in {total_batches} batches of {batch_size}")
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(message_ids))
        batch_message_ids = message_ids[start_idx:end_idx]
        
        logging.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_message_ids)} messages)")
        
        # Process this batch
        batch_results = _process_single_batch(service, batch_message_ids)
        message_full_payload_map.update(batch_results)
        
        # Add delay between batches to respect rate limits
        if batch_num < total_batches - 1:  # Don't delay after the last batch
            logging.debug(f"Waiting {delay_between_batches} seconds before next batch...")
            time.sleep(delay_between_batches)
    
    logging.info(f"Batch processing complete. Received full payloads for {len(message_full_payload_map)} messages.")
    return message_full_payload_map


def _process_single_batch(service, message_ids: List[str]) -> Dict[str, Any]:
    """
    Process a single batch of message IDs.
    
    Args:
        service: Gmail service object
        message_ids: List of message IDs to process
        
    Returns:
        Dictionary of message_id -> full payload
    """
    message_full_payload_map = {}
    
    def full_payload_callback(request_id, response, exception):
        if exception:
            logging.error(f"Failed to get full payload for {request_id}: {exception}")
            # If we get a rate limit error, we might want to retry this batch
            if "too many concurrent requests" in str(exception).lower():
                logging.warning(f"Rate limit hit for message {request_id}. Consider reducing batch size.")
        else:
            message_full_payload_map[request_id] = response
    
    batch = service.new_batch_http_request(callback=full_payload_callback)
    
    # Add requests to batch
    for msg_id in message_ids:
        request = service.users().messages().get(
            userId='me',
            id=msg_id,
            format='full'  # We want the full body and parts
        )
        batch.add(request, request_id=msg_id)
    
    try:
        batch.execute()
        logging.debug(f"Successfully processed batch of {len(message_ids)} messages")
    except Exception as e:
        logging.error(f"Batch request failed: {e}")
        # If we hit rate limits, we could implement exponential backoff here
        if "quota" in str(e).lower() or "rate" in str(e).lower():
            logging.warning("Rate limit or quota exceeded. Consider increasing delay between batches.")
    
    return message_full_payload_map


def get_attachments_messages(message_full_payload_map):
    """
    Get the valid attachments (pdfs or csv) from the full payload of the messages
    This is a parser function that will be used to extract the attachments from the messages
    """
    # List of attachments that we will make a call to the gmail API 
    attachments_messages = {} #key: attachmentId, value: [message_id, filename]

    for message_id, message_data in message_full_payload_map.items():
        attachments = []
        
        # Extract the payload from the message data
        payload = message_data.get('payload', {})
        
        # Check each part for attachments 
        parts = payload.get('parts', [])
        logging.debug(f"Parts for message {message_id}: {len(parts)} parts found")

        if not parts:
            logging.warning(f"No parts found for message {message_id}")
            continue

        for part in parts:
            attachments.extend(extract_attachments_from_payload(part))
            
            # Check nested parts (multipart messages can have nested parts)
            nested_parts = part.get('parts', [])
            for nested_part in nested_parts:
                attachments.extend(extract_attachments_from_payload(nested_part))

        if attachments:
            for attachment in attachments:
                attachments_messages[attachment['attachmentId']] = [message_id, attachment['filename']]
                logging.info(f"Found attachment: {attachment['filename']} in message {message_id}")
        else:
            logging.debug(f"No CSV/PDF attachments found in message {message_id}")
    
    return attachments_messages



def fetch_and_upload_attachments(attachments_messages, metadata_lookup, service, bucket_name, delay_between_requests=0.1):
    """
    Fetch attachment data and upload to S3 immediately to minimize memory usage.
    
    Args:
        attachments_messages: Dictionary of attachment_id -> [message_id, filename]
        metadata_lookup: Dictionary of message metadata
        service: Gmail service object
        bucket_name: S3 bucket name
        delay_between_requests: Seconds to wait between individual attachment requests
        
    Returns:
        Dictionary of attachment_id -> upload status
    """
    upload_results = {}
    total_attachments = len(attachments_messages)
    
    logging.info(f"Fetching and uploading {total_attachments} attachments with {delay_between_requests}s delay between requests")
    
    for i, (attachment_id, value) in enumerate(attachments_messages.items()):
        message_id = value[0]
        filename = value[1]
        
        logging.debug(f"Processing attachment {i+1}/{total_attachments}: {filename}")
        logging.debug(f"Attachment ID: {attachment_id}")
        logging.debug(f"Message ID: {message_id}")
        logging.debug(f"Filename: {filename}")
        
        try:
            # Fetch attachment data
            attachment = service.users().messages().attachments().get(
                userId='me',
                messageId=message_id,
                id=attachment_id
            ).execute()

            file_data = base64.urlsafe_b64decode(attachment['data'])
            
            # Create S3 key
            s3_key = create_filename(attachment_id, message_id, filename, metadata_lookup)
            
            # Upload to S3 immediately
            logging.debug(f"Uploading to S3, s3_key: {s3_key}")
            logging.debug(f"File snippet: {file_data[:20]}")
            #success = upload_to_s3(s3_key, file_data, bucket_name)

            success = True

            upload_results[attachment_id] = {
                'success': success,
                'filename': filename,
                'message_id': message_id,
                's3_key': s3_key,
                'size': len(file_data)
            }
            
            if success:
                logging.info(f"Successfully processed attachment {i+1}/{total_attachments}: {filename}")
                # TODO: uncomment this later 
                # success_add_label = add_etl_processed_label(service, message_id) 
                success_add_label = True

            if not success or not success_add_label:
                logging.error(f"Failed to upload attachment {i+1}/{total_attachments}: {filename}")
            
            # Clear file_data from memory immediately after upload
            del file_data
            
        except Exception as e:
            logging.error(f"Failed to process attachment {filename} (ID: {attachment_id}): {e}")
            upload_results[attachment_id] = {
                'success': False,
                'filename': filename,
                'message_id': message_id,
                'error': str(e)
            }
            
            if "quota" in str(e).lower() or "rate" in str(e).lower():
                logging.warning("Rate limit hit while fetching attachments. Consider increasing delay.")
                # Add extra delay if we hit rate limits
                time.sleep(2.0)
            continue
        
        # Add delay between requests to respect rate limits
        if i < total_attachments - 1:  # Don't delay after the last request
            time.sleep(delay_between_requests)
    
    successful_uploads = sum(1 for result in upload_results.values() if result.get('success', False))
    logging.info(f"Successfully processed {successful_uploads} out of {total_attachments} attachments")
    
    return upload_results
        



def main():
    # Create logs directory if it doesn't exist
    if not os.path.exists("logs"):
        os.makedirs("logs")
    
    logging.basicConfig(
        filename="logs/main.log",      
        filemode="a",                   
        level=logging.DEBUG,        
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    service = get_gmail_service()

    bucket_name = "ana-stage-v2-accounts-payable-s3-zy9l1"
    
    # Step 1: Fetch message IDs
    #TODO: redo the create_query function to not include labels 
    # query = create_query(7, ["Apex", "Jeg & Sons"])
    # print(query)

    print("Starting to fetch messages: ", datetime.now())
    messages = fetch_message_ids(
            service, 
            query="to:invoices@perpay.com newer_than:3d --label:CATEGORY_PROMOTIONS --label:CATEGORY_UPDATES --label:Zapier --label:INBOX --label:IMPORTANT --label:UNREAD --label:Zapier Alerts --label:Superseded/z-junk --label:Superseded/zRemittances")
    print(f"Fetched {len(messages)} message IDs at {datetime.now()}")

    # Get the ETL-Processed label id, this will be used to attach emails as being etl-processed
    # etl_label_id = get_or_create_label_id(service, "ETL-Processed")
    
    # Step 2: Fetch metadata in batches (smaller batches for metadata)
    message_metadata_map = get_messages_metadata_batch(service, messages, batch_size=30, delay_between_batches=0.5)
    print(f"Fetched metadata for {len(message_metadata_map)} messages at {datetime.now()}")
    filename = f"message_metadata.json"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(message_metadata_map, f, indent=4)
        print(f"Successfully saved labels to {filename}")
    except IOError as e:
        print(f"Error: Could not save file {filename}. {e}")
    except Exception as e:
        print(f"An unexpected error occurred during saving: {e}")

    # # Step 3: Create metadata lookup (filter out ETL-Processed)
    # metadata_lookup = create_metadata_lookup(message_metadata_map)
    # print(f"Created metadata lookup with {len(metadata_lookup)} emails at {datetime.now()}")
    
    # # Step 4: Fetch full payloads in smaller batches (larger delay for full payloads)
    # message_full_payload_map = get_messages_full_batch(service, metadata_lookup, batch_size=25, delay_between_batches=2.0)
    # print(f"Fetched full payloads for {len(message_full_payload_map)} messages at {datetime.now()}")

    # # Step 5: Extract attachment information
    # messages_attachments = get_attachments_messages(message_full_payload_map)
    # print(f"Found {len(messages_attachments)} attachments at {datetime.now()}")
    
    # # Step 6: Fetch and upload attachments immediately (memory optimized)
    # if messages_attachments:
    #     upload_results = fetch_and_upload_attachments(
    #         messages_attachments, 
    #         metadata_lookup, 
    #         service, 
    #         bucket_name, 
    #         delay_between_requests=0.2
    #     )
        
    #     successful_uploads = sum(1 for result in upload_results.values() if result.get('success', False))
    #     print(f"Successfully uploaded {successful_uploads} out of {len(messages_attachments)} attachments at {datetime.now()}")
        
    #     # Log any failed uploads
    #     failed_uploads = [result for result in upload_results.values() if not result.get('success', False)]
    #     if failed_uploads:
    #         print(f"Failed uploads: {len(failed_uploads)} at {datetime.now()}")
    #         for failed in failed_uploads[:5]:  # Show first 5 failures
    #             print(f"  - {failed['filename']}: {failed.get('error', 'Unknown error')}")
    # else:
    #     print("No attachments found to process")

if __name__ == "__main__":
    main()
