import boto3
import botocore
from botocore.exceptions import ClientError, ProfileNotFound, NoCredentialsError
from botocore.client import Config as BotoConfig
from typing import List, Dict, Optional, Any, Callable, Tuple
import os
import json
import keyring
import time
import webbrowser
from datetime import datetime
import threading
import queue
import json

from PyQt6.QtWidgets import QMessageBox, QInputDialog
from PyQt6.QtCore import QObject, pyqtSignal, QTimer, QEventLoop

from src.utils import get_logger, get_config, get_file_service

logger = get_logger()
config = get_config()
file_service = get_file_service()

class AWSError(Exception):
    """Custom exception for AWS errors with detailed information."""
    def __init__(self, message, code=None, operation=None, details=None):
        self.message = message
        self.code = code
        self.operation = operation
        self.details = details or {}
        super().__init__(self.message)
    
    def __str__(self):
        result = f"{self.message}"
        if self.code:
            result += f" (Code: {self.code})"
        if self.operation:
            result += f" during {self.operation}"
        return result
    
    def to_dict(self):
        return {
            'message': self.message,
            'code': self.code,
            'operation': self.operation,
            'details': self.details
        }

class AWSClient(QObject):
    """AWS client with improved error handling and retry mechanism."""
    sso_code_ready = pyqtSignal(str)  # Signal to show the SSO code
    sso_status_update = pyqtSignal(str)  # Signal for status updates
    sso_completed = pyqtSignal(bool)  # Signal to indicate authentication completion
    
    def __init__(self):
        super().__init__()
        self.session = None
        self.s3_client = None
        self.identity_store = None
        self.current_profile = None
        self._sso_timer = None
        self._sso_auth_data = None
        self.verbose_mode = False
        self._download_stop = threading.Event()
        
        # Pagination settings
        self.page_size = 1000  # Default max items per page
        self.max_pages = 20    # Maximum number of pages to fetch (for safety)
        
        # Retry configuration
        self.max_retries = 5   # Maximum number of retries
        self.retry_delay = 1   # Base delay in seconds
        self.exponential_backoff = True  # Use exponential backoff for retries
        
        # Load client settings from config
        self._load_settings()
    
    def _load_settings(self):
        """Load client settings from config."""
        self.page_size = config.get('page_size', 1000)
        self.max_retries = config.get('max_retries', 5)
        self.retry_delay = config.get('retry_delay', 1)
        self.exponential_backoff = config.get('exponential_backoff', True)
    
    def set_verbose_mode(self, verbose: bool):
        """Set verbose mode for debug output."""
        self.verbose_mode = verbose
        if verbose:
            logger.set_level("DEBUG")
    
    def _execute_with_retry(self, func, *args, **kwargs):
        """Execute a function with retry mechanism."""
        operation_name = kwargs.pop('operation_name', func.__name__)
        retries = 0
        last_exception = None
        
        while retries <= self.max_retries:
            try:
                if retries > 0:
                    # Log retry attempt
                    logger.info(f"Retry attempt {retries}/{self.max_retries} for {operation_name}")
                
                # Execute the function
                return func(*args, **kwargs)
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code')
                error_message = e.response.get('Error', {}).get('Message')
                
                # Log the error
                logger.error(f"AWS error in {operation_name}: {error_code} - {error_message}")
                
                # Check if the error is retryable
                if error_code in ['SlowDown', 'ThrottlingException', 'RequestLimitExceeded',
                                 'RequestTimeout', 'InternalError', 'ServiceUnavailable',
                                 'ProvisionedThroughputExceededException']:
                    last_exception = AWSError(
                        message=error_message,
                        code=error_code,
                        operation=operation_name,
                        details=e.response
                    )
                    retries += 1
                    
                    # Calculate delay with exponential backoff if enabled
                    if self.exponential_backoff:
                        delay = self.retry_delay * (2 ** (retries - 1))
                    else:
                        delay = self.retry_delay
                    
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    # Non-retryable error
                    raise AWSError(
                        message=error_message,
                        code=error_code,
                        operation=operation_name,
                        details=e.response
                    )
            
            except (ConnectionError, TimeoutError) as e:
                logger.error(f"Connection error in {operation_name}: {str(e)}")
                last_exception = AWSError(
                    message=str(e),
                    operation=operation_name
                )
                retries += 1
                
                # Calculate delay with exponential backoff if enabled
                if self.exponential_backoff:
                    delay = self.retry_delay * (2 ** (retries - 1))
                else:
                    delay = self.retry_delay
                
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                
            except Exception as e:
                # Log and re-raise any other exceptions
                logger.error(f"Unexpected error in {operation_name}: {str(e)}")
                raise AWSError(
                    message=str(e),
                    operation=operation_name
                )
        
        # If we've exhausted all retries, raise the last exception
        if last_exception:
            logger.error(f"All retry attempts failed for {operation_name}")
            raise last_exception
    
    def authenticate_with_access_key(self, access_key: str, secret_key: str, region: str = "us-east-1") -> bool:
        """Authenticate using AWS access key and secret key."""
        try:
            logger.info(f"Authenticating with access key in region {region}")
            
            # Configure S3 with extended timeout and retry settings
            s3_config = BotoConfig(
                region_name=region,
                signature_version='s3v4',
                retries={
                    'max_attempts': self.max_retries,
                    'mode': 'standard'
                },
                connect_timeout=30,
                read_timeout=60
            )
            
            self.session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region
            )
            
            # Create S3 client with the extended config
            self.s3_client = self.session.client('s3', config=s3_config)
            
            # Test credentials by listing buckets
            self._execute_with_retry(
                self.s3_client.list_buckets,
                operation_name="test_credentials"
            )
            
            logger.info("Authentication with access key successful")
            return True
            
        except NoCredentialsError:
            logger.error("No credentials provided")
            raise AWSError(message="No credentials provided", operation="authenticate_with_access_key")
            
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Authentication failed: {str(e)}",
                    operation="authenticate_with_access_key"
                )
    
    def authenticate_with_profile(self, profile_name: str) -> bool:
        """Authenticate using a named AWS profile."""
        try:
            logger.info(f"Authenticating with profile '{profile_name}'")
            
            self.session = boto3.Session(profile_name=profile_name)
            
            # Get the region from the profile or use default
            region = self.session.region_name or config.get('default_region', 'us-east-1')
            
            # Configure S3 with extended timeout and retry settings
            s3_config = BotoConfig(
                region_name=region,
                signature_version='s3v4',
                retries={
                    'max_attempts': self.max_retries,
                    'mode': 'standard'
                },
                connect_timeout=30,
                read_timeout=60
            )
            
            # Create S3 client with the extended config
            self.s3_client = self.session.client('s3', config=s3_config)
            
            # Set current profile
            self.current_profile = profile_name
            
            # Test credentials by listing buckets
            self._execute_with_retry(
                self.s3_client.list_buckets,
                operation_name="test_credentials"
            )
            
            logger.info(f"Authentication with profile '{profile_name}' successful")
            return True
            
        except ProfileNotFound:
            logger.error(f"Profile '{profile_name}' not found")
            raise AWSError(
                message=f"Profile '{profile_name}' not found",
                operation="authenticate_with_profile"
            )
            
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Authentication failed: {str(e)}",
                    operation="authenticate_with_profile"
                )
    
    def authenticate_with_sso(self, start_url: str, region: str, account_id: str, role_name: str) -> bool:
        """Authenticate using AWS SSO."""
        try:
            self.debug_print("Starting SSO authentication in AWSClient...")
            self.sso_status_update.emit("Initializing SSO authentication...")
            
            # First create a session with the region
            self.debug_print(f"Creating session with region: {region}")
            self.session = boto3.Session(region_name=region)
            
            # Configure SSO
            self.debug_print("Configuring SSO client...")
            sso_oidc = self.session.client('sso-oidc')
            
            # Register the client
            self.debug_print("Registering SSO client...")
            self.sso_status_update.emit("Registering SSO client...")
            client_creds = sso_oidc.register_client(
                clientName='s3xplorer',
                clientType='public',
                scopes=['sso:account:access']
            )
            self.debug_print("SSO client registered successfully")
            
            # Get authorization URL
            self.debug_print("Requesting authorization URL...")
            self.sso_status_update.emit("Requesting authorization...")
            auth_url = sso_oidc.start_device_authorization(
                clientId=client_creds['clientId'],
                clientSecret=client_creds['clientSecret'],
                startUrl=start_url
            )
            self.debug_print("Authorization URL received")
            
            # Show the authorization code to the user
            user_code = auth_url['userCode']
            self.debug_print(f"Authorization code: {user_code}")
            self.sso_code_ready.emit(user_code)
            
            # Open the browser for authorization
            self.debug_print("Opening browser for authorization...")
            self.sso_status_update.emit("Opening browser for authorization...")
            verification_url = auth_url['verificationUriComplete']
            self.debug_print(f"Opening verification URL: {verification_url}")
            webbrowser.open(verification_url)
            
            # Store auth data for polling
            self._sso_auth_data = {
                'sso_oidc': sso_oidc,
                'client_creds': client_creds,
                'auth_url': auth_url,
                'start_time': time.time(),
                'region': region,
                'account_id': account_id,
                'role_name': role_name
            }
            
            # Start polling timer
            self._sso_timer = QTimer()
            self._sso_timer.timeout.connect(self._poll_sso_token)
            self._sso_timer.start(auth_url['interval'] * 1000)  # Convert to milliseconds
            
            # Create event loop to wait for completion
            loop = QEventLoop()
            self.sso_completed.connect(loop.quit)
            loop.exec()
            
            return True
            
        except Exception as e:
            self.debug_print(f"SSO authentication failed: {str(e)}")
            self.sso_status_update.emit(f"SSO authentication failed: {str(e)}")
            self.sso_completed.emit(False)
            return False
    
    def _poll_sso_token(self):
        """Poll for SSO token using Qt timer."""
        try:
            if not self._sso_auth_data:
                return
                
            data = self._sso_auth_data
            self.debug_print("Attempting to create token...")
            
            try:
                token = data['sso_oidc'].create_token(
                    clientId=data['client_creds']['clientId'],
                    clientSecret=data['client_creds']['clientSecret'],
                    grantType='urn:ietf:params:oauth:grant-type:device_code',
                    deviceCode=data['auth_url']['deviceCode']
                )
                self.debug_print("Token created successfully")
                
                # Get credentials
                self.debug_print("Getting role credentials...")
                self.sso_status_update.emit("Getting credentials...")
                sso = self.session.client('sso')
                credentials = sso.get_role_credentials(
                    roleName=data['role_name'],
                    accountId=data['account_id'],
                    accessToken=token['accessToken']
                )
                self.debug_print("Role credentials obtained")
                
                # Create session with SSO credentials
                self.debug_print("Creating session with SSO credentials...")
                self.session = boto3.Session(
                    aws_access_key_id=credentials['roleCredentials']['accessKeyId'],
                    aws_secret_access_key=credentials['roleCredentials']['secretAccessKey'],
                    aws_session_token=credentials['roleCredentials']['sessionToken'],
                    region_name=data['region']
                )
                
                # Verify the credentials work by creating the S3 client
                self.debug_print("Creating S3 client...")
                self.s3_client = self.session.client('s3')
                try:
                    # Test the credentials by listing buckets
                    self.debug_print("Testing credentials by listing buckets...")
                    self.s3_client.list_buckets()
                    self.debug_print("SSO authentication successful!")
                    self.sso_status_update.emit("SSO authentication successful!")
                    
                    # Clean up
                    if self._sso_timer:
                        self._sso_timer.stop()
                    self._sso_auth_data = None
                    
                    self.sso_completed.emit(True)
                except Exception as e:
                    self.debug_print(f"Failed to verify credentials: {str(e)}")
                    raise Exception(f"Failed to verify credentials: {str(e)}")
                    
            except ClientError as e:
                if e.response['Error']['Code'] == 'AuthorizationPendingException':
                    if time.time() - data['start_time'] > data['auth_url']['expiresIn']:
                        self.debug_print("Authorization timed out")
                        if self._sso_timer:
                            self._sso_timer.stop()
                        self._sso_auth_data = None
                        raise Exception("Authorization timed out. Please try again.")
                    return  # Continue polling
                self.debug_print(f"Error creating token: {str(e)}")
                raise
                
        except Exception as e:
            self.debug_print(f"Error in token polling: {str(e)}")
            if self._sso_timer:
                self._sso_timer.stop()
            self._sso_auth_data = None
            self.sso_status_update.emit(f"SSO authentication failed: {str(e)}")
            self.sso_completed.emit(False)
    
    def list_buckets(self) -> List[Dict[str, Any]]:
        """List all S3 buckets."""
        try:
            logger.info("Listing S3 buckets")
            
            response = self._execute_with_retry(
                self.s3_client.list_buckets,
                operation_name="list_buckets"
            )
            
            # Format the response
            buckets = []
            for bucket in response.get('Buckets', []):
                buckets.append({
                    'name': bucket.get('Name', ''),
                    'creation_date': bucket.get('CreationDate', datetime.now()).isoformat()
                })
            
            logger.info(f"Found {len(buckets)} buckets")
            return buckets
            
        except Exception as e:
            logger.error(f"Error listing buckets: {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Failed to list buckets: {str(e)}",
                    operation="list_buckets"
                )
    
    def list_objects(self, bucket: str, prefix: str = '', delimiter: str = '/') -> Dict[str, Any]:
        """List objects in an S3 bucket with pagination support."""
        try:
            logger.info(f"Listing objects in bucket '{bucket}' with prefix '{prefix}'")
            
            # Parameters for the list_objects_v2 call
            params = {
                'Bucket': bucket,
                'MaxKeys': self.page_size,
                'Delimiter': delimiter
            }
            
            if prefix:
                params['Prefix'] = prefix
            
            # Initialize result containers
            all_objects = []
            all_prefixes = []
            
            # Handle pagination
            continuation_token = None
            page_count = 0
            
            while True:
                page_count += 1
                
                # Add continuation token if we have one
                if continuation_token:
                    params['ContinuationToken'] = continuation_token
                
                # Make the API call
                response = self._execute_with_retry(
                    self.s3_client.list_objects_v2,
                    **params,
                    operation_name="list_objects"
                )
                
                # Process objects
                for obj in response.get('Contents', []):
                    all_objects.append({
                        'key': obj.get('Key', ''),
                        'size': obj.get('Size', 0),
                        'last_modified': obj.get('LastModified', datetime.now()).isoformat(),
                        'etag': obj.get('ETag', '').strip('"'),
                        'storage_class': obj.get('StorageClass', 'STANDARD'),
                        'is_directory': obj.get('Key', '').endswith('/')
                    })
                
                # Process prefixes (directories)
                for prefix_obj in response.get('CommonPrefixes', []):
                    prefix_value = prefix_obj.get('Prefix', '')
                    # Only add it if it's not just the current prefix
                    if prefix_value and prefix_value != prefix:
                        all_prefixes.append({
                            'prefix': prefix_value,
                            'name': prefix_value.rstrip('/').split('/')[-1] if '/' in prefix_value else prefix_value.rstrip('/')
                        })
                
                # Check if there are more pages
                if not response.get('IsTruncated', False) or page_count >= self.max_pages:
                    break
                
                # Get the continuation token for the next page
                continuation_token = response.get('NextContinuationToken')
            
            logger.info(f"Found {len(all_objects)} objects and {len(all_prefixes)} directories")
            
            return {
                'objects': all_objects,
                'prefixes': all_prefixes,
                'bucket': bucket,
                'current_prefix': prefix
            }
            
        except Exception as e:
            logger.error(f"Error listing objects in bucket '{bucket}': {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Failed to list objects in bucket '{bucket}': {str(e)}",
                    operation="list_objects",
                    details={'bucket': bucket, 'prefix': prefix}
                )
    
    def upload_file(self, file_path: str, bucket: str, key: str, 
                    extra_args: Dict[str, Any] = None,
                    progress_callback: Callable[[int], bool] = None) -> bool:
        """
        Upload a file to S3 with progress tracking and multipart for large files.
        
        Args:
            file_path: Local file path to upload
            bucket: S3 bucket name
            key: S3 object key
            extra_args: Additional arguments for the upload (content type, metadata, etc.)
            progress_callback: Callback for progress updates, returns False to cancel
            
        Returns:
            bool: Success or failure
        """
        try:
            # Validate file exists
            if not os.path.exists(file_path):
                raise AWSError(
                    message=f"File not found: {file_path}",
                    operation="upload_file"
                )
            
            file_size = os.path.getsize(file_path)
            logger.info(f"Uploading file '{file_path}' ({file_size} bytes) to {bucket}/{key}")
            
            # Create a transfer config for multipart uploads
            config = boto3.s3.transfer.TransferConfig(
                multipart_threshold=8 * 1024 * 1024,  # 8MB
                max_concurrency=10,
                multipart_chunksize=8 * 1024 * 1024,  # 8MB
                use_threads=True
            )
            
            # Set up callback handler for progress
            class ProgressCallback:
                def __init__(self, callback_func):
                    self.callback_func = callback_func
                    self.total_size = file_size
                    self.uploaded = 0
                    self.last_percent = 0
                
                def __call__(self, bytes_amount):
                    self.uploaded += bytes_amount
                    percent = int(min(100, self.uploaded * 100 / self.total_size))
                    
                    # Only call the callback if progress has changed by at least 1%
                    if percent > self.last_percent:
                        self.last_percent = percent
                        # If callback exists and returns False, cancel the upload
                        if self.callback_func and not self.callback_func(percent):
                            return False
                    
                    return True
            
            progress_tracker = ProgressCallback(progress_callback) if progress_callback else None
            callback_kwargs = {'Callback': progress_tracker} if progress_tracker else {}
            
            # Extra args for the upload
            upload_args = extra_args or {}
            
            # Execute the upload with automatic multipart handling
            self.s3_client.upload_file(
                file_path, 
                bucket, 
                key,
                Config=config,
                ExtraArgs=upload_args,
                **callback_kwargs
            )
            
            logger.info(f"Successfully uploaded {file_path} to {bucket}/{key}")
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            error_message = e.response.get('Error', {}).get('Message')
            logger.error(f"S3 upload error: {error_code} - {error_message}")
            raise AWSError(
                message=error_message or "Upload failed",
                code=error_code,
                operation="upload_file",
                details={'bucket': bucket, 'key': key}
            )
            
        except Exception as e:
            logger.error(f"Error uploading file: {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Upload failed: {str(e)}",
                    operation="upload_file",
                    details={'bucket': bucket, 'key': key}
                )
    
    def download_file(self, bucket: str, key: str, save_path: str,
                     progress_callback: Callable[[int], bool] = None) -> bool:
        """
        Download a file from S3 with progress tracking.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            save_path: Local path to save the file
            progress_callback: Callback for progress updates, returns False to cancel
            
        Returns:
            bool: Success or failure
        """
        try:
            logger.info(f"Downloading {bucket}/{key} to {save_path}")
            
            # Reset download stop event
            self._download_stop.clear()
            
            # Get object metadata to determine size
            metadata = self._execute_with_retry(
                self.s3_client.head_object,
                Bucket=bucket,
                Key=key,
                operation_name="head_object"
            )
            
            file_size = metadata.get('ContentLength', 0)
            
            # Create a transfer config
            config = boto3.s3.transfer.TransferConfig(
                multipart_threshold=8 * 1024 * 1024,  # 8MB
                max_concurrency=10,
                multipart_chunksize=8 * 1024 * 1024,  # 8MB
                use_threads=True
            )
            
            # Set up callback handler for progress
            class ProgressCallback:
                def __init__(self, callback_func, stop_event):
                    self.callback_func = callback_func
                    self.stop_event = stop_event
                    self.total_size = file_size
                    self.downloaded = 0
                    self.last_percent = 0
                
                def __call__(self, bytes_amount):
                    if self.stop_event.is_set():
                        return False
                    
                    self.downloaded += bytes_amount
                    percent = int(min(100, self.downloaded * 100 / self.total_size)) if self.total_size else 0
                    
                    # Only call the callback if progress has changed by at least 1%
                    if percent > self.last_percent:
                        self.last_percent = percent
                        # If callback exists and returns False, cancel the download
                        if self.callback_func and not self.callback_func(percent):
                            self.stop_event.set()
                            return False
                    
                    return True
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            progress_tracker = ProgressCallback(progress_callback, self._download_stop) if progress_callback else None
            callback_kwargs = {'Callback': progress_tracker} if progress_tracker else {}
            
            # Execute the download with automatic multipart handling
            self.s3_client.download_file(
                bucket, 
                key, 
                save_path,
                Config=config,
                **callback_kwargs
            )
            
            if self._download_stop.is_set():
                logger.info(f"Download of {bucket}/{key} was cancelled")
                if os.path.exists(save_path):
                    try:
                        os.remove(save_path)
                    except:
                        pass
                return False
            
            logger.info(f"Successfully downloaded {bucket}/{key} to {save_path}")
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            error_message = e.response.get('Error', {}).get('Message')
            logger.error(f"S3 download error: {error_code} - {error_message}")
            
            # Clean up partial download
            if os.path.exists(save_path):
                try:
                    os.remove(save_path)
                except:
                    pass
            
            raise AWSError(
                message=error_message or "Download failed",
                code=error_code,
                operation="download_file",
                details={'bucket': bucket, 'key': key}
            )
            
        except Exception as e:
            logger.error(f"Error downloading file: {str(e)}")
            
            # Clean up partial download
            if os.path.exists(save_path):
                try:
                    os.remove(save_path)
                except:
                    pass
            
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Download failed: {str(e)}",
                    operation="download_file",
                    details={'bucket': bucket, 'key': key}
                )
    
    def delete_object(self, bucket: str, key: str) -> bool:
        """Delete an object from S3."""
        try:
            logger.info(f"Deleting object {bucket}/{key}")
            
            self._execute_with_retry(
                self.s3_client.delete_object,
                Bucket=bucket,
                Key=key,
                operation_name="delete_object"
            )
            
            logger.info(f"Successfully deleted {bucket}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting object {bucket}/{key}: {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Delete failed: {str(e)}",
                    operation="delete_object",
                    details={'bucket': bucket, 'key': key}
                )
    
    def cancel_download(self):
        """Cancel an ongoing download."""
        logger.info("Cancelling download")
        self._download_stop.set()
    
    def get_object_url(self, bucket: str, key: str, expiration: int = 3600) -> str:
        """Generate a pre-signed URL for an object."""
        try:
            logger.info(f"Generating pre-signed URL for {bucket}/{key}")
            
            url = self._execute_with_retry(
                self.s3_client.generate_presigned_url,
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket,
                    'Key': key
                },
                ExpiresIn=expiration,
                operation_name="generate_presigned_url"
            )
            
            logger.info(f"URL generated for {bucket}/{key}")
            return url
            
        except Exception as e:
            logger.error(f"Error generating URL for {bucket}/{key}: {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"URL generation failed: {str(e)}",
                    operation="get_object_url",
                    details={'bucket': bucket, 'key': key}
                )
    
    def get_object_metadata(self, bucket: str, key: str) -> Dict[str, Any]:
        """Get metadata for an S3 object."""
        try:
            logger.info(f"Getting metadata for {bucket}/{key}")
            
            response = self._execute_with_retry(
                self.s3_client.head_object,
                Bucket=bucket,
                Key=key,
                operation_name="head_object"
            )
            
            # Extract and format metadata
            metadata = {
                'content_type': response.get('ContentType', 'application/octet-stream'),
                'content_length': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified', datetime.now()).isoformat(),
                'etag': response.get('ETag', '').strip('"'),
                'storage_class': response.get('StorageClass', 'STANDARD'),
                'metadata': response.get('Metadata', {})
            }
            
            logger.info(f"Retrieved metadata for {bucket}/{key}")
            return metadata
            
        except Exception as e:
            logger.error(f"Error getting metadata for {bucket}/{key}: {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Failed to get metadata: {str(e)}",
                    operation="get_object_metadata",
                    details={'bucket': bucket, 'key': key}
                )
    
    def copy_object(self, source_bucket: str, source_key: str, 
                  dest_bucket: str, dest_key: str) -> bool:
        """Copy an object within S3."""
        try:
            logger.info(f"Copying {source_bucket}/{source_key} to {dest_bucket}/{dest_key}")
            
            copy_source = {
                'Bucket': source_bucket,
                'Key': source_key
            }
            
            self._execute_with_retry(
                self.s3_client.copy_object,
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key,
                operation_name="copy_object"
            )
            
            logger.info(f"Successfully copied {source_bucket}/{source_key} to {dest_bucket}/{dest_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error copying object: {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Copy failed: {str(e)}",
                    operation="copy_object",
                    details={
                        'source_bucket': source_bucket,
                        'source_key': source_key,
                        'dest_bucket': dest_bucket,
                        'dest_key': dest_key
                    }
                )
    
    def create_bucket(self, bucket_name: str, region: str = None) -> bool:
        """Create a new S3 bucket."""
        try:
            logger.info(f"Creating bucket {bucket_name}")
            
            # If no region specified, use the client's region
            if not region and self.session:
                region = self.session.region_name
            
            # Create the bucket
            params = {'Bucket': bucket_name}
            
            # If region is not the default us-east-1, add LocationConstraint
            if region and region != 'us-east-1':
                params['CreateBucketConfiguration'] = {
                    'LocationConstraint': region
                }
            
            self._execute_with_retry(
                self.s3_client.create_bucket,
                **params,
                operation_name="create_bucket"
            )
            
            logger.info(f"Successfully created bucket {bucket_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating bucket {bucket_name}: {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Bucket creation failed: {str(e)}",
                    operation="create_bucket",
                    details={'bucket': bucket_name, 'region': region}
                )
    
    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """Delete an S3 bucket."""
        try:
            logger.info(f"Deleting bucket {bucket_name}")
            
            # If force is True, delete all objects first
            if force:
                logger.info(f"Force delete requested for {bucket_name}, removing all objects")
                self._delete_all_objects(bucket_name)
            
            # Delete the bucket
            self._execute_with_retry(
                self.s3_client.delete_bucket,
                Bucket=bucket_name,
                operation_name="delete_bucket"
            )
            
            logger.info(f"Successfully deleted bucket {bucket_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting bucket {bucket_name}: {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Bucket deletion failed: {str(e)}",
                    operation="delete_bucket",
                    details={'bucket': bucket_name}
                )
    
    def _delete_all_objects(self, bucket_name: str) -> bool:
        """Delete all objects in a bucket."""
        try:
            logger.info(f"Deleting all objects in bucket {bucket_name}")
            
            # List all objects
            result = self.list_objects(bucket_name)
            objects = result.get('objects', [])
            
            if not objects:
                logger.info(f"Bucket {bucket_name} is already empty")
                return True
            
            # Delete objects in batches
            batch_size = 1000  # Max objects per delete operation
            for i in range(0, len(objects), batch_size):
                batch = objects[i:i+batch_size]
                
                delete_keys = {'Objects': [{'Key': obj['key']} for obj in batch]}
                
                self._execute_with_retry(
                    self.s3_client.delete_objects,
                    Bucket=bucket_name,
                    Delete=delete_keys,
                    operation_name="delete_objects"
                )
            
            logger.info(f"Successfully deleted {len(objects)} objects from {bucket_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting objects from {bucket_name}: {str(e)}")
            if isinstance(e, AWSError):
                raise
            else:
                raise AWSError(
                    message=f"Failed to delete objects: {str(e)}",
                    operation="_delete_all_objects",
                    details={'bucket': bucket_name}
                ) 