from dotenv import load_dotenv
import boto3
import requests
import logging
import os
import folder_paths
from typing import List
from botocore.exceptions import ClientError

load_dotenv()

def get_output_image_paths(output_directory, history_result) -> List[str]:
    output_paths = []
    output_images = history_result['outputs']
    for node_id in output_images:
        images_arr = output_images[node_id]['images']

        for image_dict in images_arr:
            filename = image_dict['filename']
            subfolder = image_dict['subfolder']

            if subfolder == '':
                image_full_path = os.path.join(output_directory, filename)
            else:
                image_full_path = os.path.join(os.path.join(output_directory, subfolder), filename)
            output_paths.append(image_full_path)
    
    return output_paths

def get_s3_bucket():
    # print("s3_connection_atempt", os.getenv('S3_BUCKET_REGION'), os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY'))
    s3_client = boto3.resource(
        service_name='s3',
        region_name=os.getenv('S3_BUCKET_REGION'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    bucket = s3_client.Bucket(os.getenv('S3_BUCKET_NAME'))

    return bucket           

def upload_file_to_s3(file_name, local_path, s3_path, s3_bucket):
    try:
        full_local_path = os.path.join(local_path, file_name)
        full_s3_path = os.path.join(s3_path, file_name)
        
        # Perform the file upload
        s3_bucket.upload_file(Filename=full_local_path, Key=full_s3_path)
        
        # Return True if successful
        return True
    except ClientError as e:
        # Handle any exceptions (e.g., access denied, bucket not found)
        print(f"Error uploading file: {e}")
        return False

def upload_multiple_files(local_full_paths, s3_path):
    s3_bucket = get_s3_bucket()
    results = []
    for local_full_path in local_full_paths:
        local_folder_path = os.path.dirname(local_full_path)
        file_name = os.path.basename(local_full_path)
        result = upload_file_to_s3(file_name, local_folder_path, s3_path, s3_bucket)
        results.append(result)
    
    return all(results)  # Returns True if all uploads succeed

def handle_prompt_execution_start(prompt_id, extra_data):
    if "on_start" in extra_data:
        on_start_data = extra_data["on_start_data"]
        if "url" in on_start_data and "endpoint" in on_start_data:
            server_url = on_start_data["url"]
            endpoint = on_start_data["endpoint"]
            json_data = {"prompt_id": prompt_id}
            logging.info(f"Prompt start - notifying server at {server_url}/{endpoint}", json_data)
            response = requests.post(f"{server_url}/{endpoint}", json=json_data)
            return response

def handle_output_data(prompt_id, extra_data, history_result):
    logging.info(f"Prompt {prompt_id} finished, posting to python server...")
    
    # Upload all output images to S3
    output_image_paths = get_output_image_paths(folder_paths.get_output_directory(), history_result)
    output_path = None
    if "output_base_path" in extra_data:
        output_base_path = extra_data["output_base_path"]
        output_path = os.path.join(output_base_path, prompt_id)
        logging.info(f"Uploading output files to s3 {output_path}")
        res = upload_multiple_files(output_image_paths, output_path)
  
    # Notify the server that the outputs have been uploaded to S3 (successfully or not)
    # print("output_image_paths", output_image_paths)
    if "on_completion" in extra_data:
        completion_data = extra_data["on_completion"]
        if "url" in completion_data and "endpoint" in completion_data:
            server_url = completion_data["url"]
            endpoint = completion_data["endpoint"]
            json_data = {"prompt_id": prompt_id, "output_path": output_path, "success": res}
            logging.info(f"Notifying server at {server_url}/{endpoint}", json_data)
            response = requests.post(f"{server_url}/{endpoint}", json=json_data)
            return response