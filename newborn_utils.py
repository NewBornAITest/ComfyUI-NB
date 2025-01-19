from dotenv import load_dotenv
import boto3
import requests
import logging
import os
import folder_paths
from typing import List
from botocore.exceptions import ClientError
from enum import Enum
from google.cloud import secretmanager

load_dotenv()


class WorkflowTypes(str, Enum):
    TTI = "TTI"
    VTON = "VTON"
    UNKNOWN = "UNKNOWN"


WORKFLOW_ESTIMATES = {
    WorkflowTypes.TTI: 10,  # Example time in minutes
    WorkflowTypes.VTON: 130,
    WorkflowTypes.UNKNOWN: 0  # Default time for unknown job types
}


def access_secret(secret_id, version_id="latest"):
    project_id = "sdvesti-infrastructure"
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def get_vm_id():
    # URL to fetch the instance ID from the metadata server
    metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/id"
    headers = {"Metadata-Flavor": "Google"}

    # Fetch the instance ID
    response = requests.get(metadata_url, headers=headers)
    response.raise_for_status()  # Raise an error if the request fails

    return response.text


def get_vm_zone():
    # Send a GET request to the metadata server to retrieve the zone
    url = 'http://metadata.google.internal/computeMetadata/v1/instance/zone'
    headers = {'Metadata-Flavor': 'Google'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    zone = response.text.split('/')[-1]

    return zone


def get_project_id():
    url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    headers = {"Metadata-Flavor": "Google"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception("Failed to get project ID from metadata server")


class NewBornUtils:
    def __init__(self):
        print("NewBornUtils instance initialized...")

    def get_output_image_paths(self, output_directory, history_result) -> List[str]:
        output_paths = []
        output_images = history_result['outputs']
        for node_id in output_images:
            images_arr = output_images[node_id]['images']

            for image_dict in images_arr:
                filename = image_dict['filename']
                subfolder = image_dict['subfolder']
                image_type = image_dict['type']

                if image_type == "output":
                    if subfolder == '':
                        image_full_path = os.path.join(output_directory, filename)
                    else:
                        image_full_path = os.path.join(os.path.join(output_directory, subfolder), filename)
                    output_paths.append(image_full_path)

        return output_paths

    # def get_s3_bucket(self):
    #     s3_client = boto3.resource(
    #         service_name='s3',
    #         region_name=os.getenv('S3_BUCKET_REGION'),
    #         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    #         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    #     )
    #     bucket = s3_client.Bucket(os.getenv('S3_BUCKET_NAME'))
    #     return bucket

    def get_s3_bucket(self):
        try:
            print("Getting S3 bucket...")
            region = access_secret('S3_BUCKET_REGION')
            print("Region", region)
            aws_access_key_id = access_secret('AWS_ACCESS_KEY_ID')
            aws_secret_access_key = access_secret('AWS_SECRET_ACCESS_KEY')
            bucket_name = access_secret('S3_BUCKET_NAME')

            # Check if any of the secrets are None (i.e., failed to retrieve)
            if not all([region, aws_access_key_id, aws_secret_access_key, bucket_name]):
                raise ValueError("One or more secrets could not be retrieved.")

            # Initialize the S3 client with the retrieved credentials
            s3_client = boto3.resource(
                service_name='s3',
                region_name=region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )

            # Get the S3 bucket
            bucket = s3_client.Bucket(bucket_name)
            return bucket

        except ValueError as ve:
            print(f"ValueError: {ve}")
        except Exception as e:
            print(f"An error occurred while retrieving the S3 bucket: {e}")

    def upload_file_to_s3(self, file_name, local_path, s3_path, s3_bucket):
        try:
            full_local_path = os.path.join(local_path, file_name)
            full_s3_path = os.path.join(s3_path, file_name)

            s3_bucket.upload_file(Filename=full_local_path, Key=full_s3_path)

            res = {
                "local_path": full_local_path,
                "storage_path": full_s3_path
            }
            return res
        except ClientError as e:
            logging.error(f"Error uploading file: {e}")
            return False

    def upload_multiple_files(self, local_full_paths, s3_path):
        s3_bucket = self.get_s3_bucket()
        results = []
        for local_full_path in local_full_paths:
            local_folder_path = os.path.dirname(local_full_path)
            file_name = os.path.basename(local_full_path)
            result = self.upload_file_to_s3(file_name, local_folder_path, s3_path, s3_bucket)
            results.append(result)

        return results

    def handle_prompt_enqueue(self, prompt_id, extra_data):
        if "on_enqueue" in extra_data:
            on_enqueue_data = extra_data["on_enqueue"]
            if "url" in on_enqueue_data and "endpoint" in on_enqueue_data:
                server_url = on_enqueue_data["url"]
                endpoint = on_enqueue_data["endpoint"]
                json_data = {"prompt_id": prompt_id}
                logging.info(f"Prompt inserted to queue - notifying server at {server_url}/{endpoint}/{prompt_id}",
                             json_data)
                response = requests.post(f"{server_url}/{endpoint}/{prompt_id}", json=json_data)
                return response

    def handle_prompt_execution_start(self, prompt_id, extra_data):
        if "on_start" in extra_data:
            on_start_data = extra_data["on_start"]
            if "url" in on_start_data and "endpoint" in on_start_data:
                server_url = on_start_data["url"]
                endpoint = on_start_data["endpoint"]
                json_data = {"prompt_id": prompt_id}
                logging.info(f"Prompt start - notifying server at {server_url}/{endpoint}/{prompt_id}", json_data)
                response = requests.post(f"{server_url}/{endpoint}/{prompt_id}", json=json_data)
                return response

    def handle_prompt_complete(self, prompt_id, extra_data, history_result, execution_time=-1):
        logging.info(f"Prompt {prompt_id} finished, posting to python server...")

        output_image_paths = self.get_output_image_paths(folder_paths.get_output_directory(), history_result)
        output_path = None
        if "output_base_path" in extra_data:
            output_base_path = extra_data["output_base_path"]
            output_path = os.path.join(output_base_path, prompt_id)
            logging.info(f"Uploading output files to s3 {output_path}")
            upload_path_tuples = self.upload_multiple_files(output_image_paths, output_path)
            upload_success = all(upload_path_tuples)

        if "on_completion" in extra_data:
            completion_data = extra_data["on_completion"]
            if "url" in completion_data and "endpoint" in completion_data:
                server_url = completion_data["url"]
                endpoint = completion_data["endpoint"]
                json_data = {
                    "prompt_id": prompt_id,
                    "output": upload_path_tuples,
                    "success": upload_success,
                    "execution_time": execution_time
                }
                logging.info(f"Notifying server at {server_url}/{endpoint}/{prompt_id}", json_data)
                response = requests.post(f"{server_url}/{endpoint}/{prompt_id}", json=json_data)
                return response

    def get_queue_jobs_info(self, queue):
        jobs_info = {"types": [], "estimate": 0}
        for item in queue:
            if len(item) >= 4 and isinstance(item[2], dict):
                type_value = item[3].get('type', 'UNKNOWN')
                job_type = WorkflowTypes(type_value)
                jobs_info["types"].append(job_type)
                jobs_info["estimate"] += WORKFLOW_ESTIMATES.get(job_type, 0)
            else:
                jobs_info["types"].append(WorkflowTypes.UNKNOWN)
                jobs_info["estimate"] += WORKFLOW_ESTIMATES.get(WorkflowTypes.UNKNOWN, 0)

        return jobs_info

    def get_queue_estimate(self, queue_jobs):
        queue_running = queue_jobs['queue_running']
        queue_pending = queue_jobs['queue_pending']

        running_jobs_info = self.get_queue_jobs_info(queue_running)
        pending_jobs_info = self.get_queue_jobs_info(queue_pending)

        return {"running": running_jobs_info, "pending": pending_jobs_info}

    def publish_queue_estimate_metric(self, jobs_info):
        VM_id = get_vm_id()
        zone = get_vm_zone()

        logging.info("Publishing metric to GCP...")
        server_url = "https://newborn-backend-dot-newbornai-test-436709.lm.r.appspot.com"
        endpoint = "tasks"
        json_data = {
            "jobs_info": jobs_info,
            "zone": zone
        }
        logging.info(f"Notifying server at {server_url}/{endpoint}/{VM_id}", json_data)
        response = requests.post(f"{server_url}/{endpoint}/{VM_id}", json=json_data)

    def handle_queue_changed(self, queue_jobs):
        jobs_info = self.get_queue_estimate(queue_jobs)
        self.publish_queue_estimate_metric(jobs_info)
