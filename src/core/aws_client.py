import boto3
import botocore
from botocore.exceptions import ClientError, ProfileNotFound
from typing import List, Dict, Optional, Any, Callable
import os
import json
import keyring
import time
import webbrowser
from datetime import datetime
from PyQt6.QtWidgets import QMessageBox, QInputDialog
from PyQt6.QtCore import QObject, pyqtSignal, QTimer, QEventLoop
import threading
import queue

class AWSClient(QObject):
    """AWS client with SSO support."""
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
    
    def set_verbose_mode(self, verbose: bool):
        """Set verbose mode for debug output."""
        self.verbose_mode = verbose
    
    def debug_print(self, message: str):
        """Print debug message if verbose mode is enabled."""
        if self.verbose_mode:
            print(message)
    
    def authenticate_with_access_key(self, access_key: str, secret_key: str, region: str = "us-east-1") -> bool:
        """Authenticate using AWS access key and secret key."""
        try:
            self.session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region
            )
            self.s3_client = self.session.client('s3')
            return True
        except Exception as e:
            print(f"Authentication failed: {str(e)}")
            return False
    
    def authenticate_with_profile(self, profile_name: str) -> bool:
        """Authenticate using a named AWS profile."""
        try:
            self.session = boto3.Session(profile_name=profile_name)
            self.s3_client = self.session.client('s3')
            self.current_profile = profile_name
            return True
        except ProfileNotFound:
            print(f"Profile '{profile_name}' not found")
            return False
        except Exception as e:
            print(f"Authentication failed: {str(e)}")
            return False
    
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
        if not self.s3_client:
            raise Exception("Not authenticated")
        
        try:
            response = self.s3_client.list_buckets()
            return response['Buckets']
        except ClientError as e:
            print(f"Error listing buckets: {str(e)}")
            return []
    
    def list_objects(self, bucket: str, prefix: str = "") -> List[Dict[str, Any]]:
        """List objects in a bucket with optional prefix."""
        if not self.s3_client:
            raise Exception("Not authenticated")
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            objects = []
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    objects.extend(page['Contents'])
            
            return objects
        except ClientError as e:
            print(f"Error listing objects: {str(e)}")
            return []
    
    def upload_file(self, file_path: str, bucket: str, key: str, progress_callback: Optional[Callable[[int], bool]] = None) -> bool:
        """Upload a file to S3 with progress tracking and cancellation support."""
        try:
            # Get file size for progress calculation
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                self.debug_print("Warning: File size reported as 0.")
                # Handle 0-byte files by just uploading them
                if progress_callback:
                    progress_callback(0)
                self.s3_client.upload_file(file_path, bucket, key)
                return True

            uploaded_size = 0
            cancel_requested = False

            def callback(bytes_amount):
                nonlocal uploaded_size, cancel_requested
                # Check if cancellation was already requested
                if cancel_requested:
                    raise RuntimeError("Upload cancelled by user callback.")

                uploaded_size += bytes_amount
                if progress_callback and file_size > 0:
                    progress = int((uploaded_size / file_size) * 100)
                    # Clamp progress between 0 and 100
                    progress = max(0, min(100, progress))
                    
                    # Call the provided progress_callback (which checks worker's is_cancelled)
                    if not progress_callback(progress):
                        self.debug_print("AWSClient: progress_callback returned False. Requesting cancel.")
                        cancel_requested = True
                        # Raise exception immediately to try and stop the transfer
                        raise RuntimeError("Upload cancelled by user callback.")
                elif progress_callback:  # Handle 0-byte files or files where size wasn't obtained
                    progress_callback(0)  # Report 0 progress, but check cancellation
                    if cancel_requested:  # Check again in case callback set it
                        raise RuntimeError("Upload cancelled by user callback.")

            # Configure transfer - use threads=False for simplicity and stability
            config = boto3.s3.transfer.TransferConfig(
                use_threads=False  # Let's try disabling internal threading first
            )

            self.debug_print(f"Starting boto3 upload_file for {file_path} to {bucket}/{key}")
            self.s3_client.upload_file(
                file_path,
                bucket,
                key,
                Callback=callback,
                Config=config
            )
            
            # If we reach here without the exception, the upload finished before cancellation took effect
            # Or it finished successfully.
            self.debug_print("boto3 upload_file completed without cancellation exception.")
            return True

        except RuntimeError as e:
            # Catch our specific cancellation exception
            if str(e) == "Upload cancelled by user callback.":
                self.debug_print("AWSClient: Caught cancellation exception. Upload stopped.")
                return False
            else:
                # Different runtime error
                self.debug_print(f"AWSClient: Upload failed with runtime error: {e}")
                return False
        except Exception as e:
            # Catch other exceptions (ClientError, etc.)
            self.debug_print(f"AWSClient: Upload failed with error: {str(e)}")
            return False
    
    def download_file(self, bucket: str, key: str, save_path: str, progress_callback: Optional[Callable[[int], bool]] = None) -> bool:
        """Download a file from S3 using boto3's download_file with cancellation support via callback."""
        f = None # Keep track of file handle for potential cleanup
        try:
            # Get object size for progress calculation
            self.debug_print(f"Getting metadata for {bucket}/{key}")
            head_response = self.s3_client.head_object(Bucket=bucket, Key=key)
            file_size = head_response.get('ContentLength', 0)
            if file_size == 0:
                 self.debug_print("Warning: File size reported as 0.")
                 # Decide how to handle 0-byte files, maybe download anyway? For now, proceed.

            downloaded_size = 0
            cancel_requested = False

            def callback(bytes_amount):
                nonlocal downloaded_size, cancel_requested
                # Check if cancellation was already requested by the progress callback
                if cancel_requested:
                    # We need to raise an exception here to stop boto3's transfer manager
                    raise RuntimeError("Download cancelled by user callback.")

                downloaded_size += bytes_amount
                if progress_callback and file_size > 0:
                    progress = int((downloaded_size / file_size) * 100)
                    # Clamp progress between 0 and 100
                    progress = max(0, min(100, progress))
                    
                    # Call the provided progress_callback (which checks worker's is_cancelled)
                    if not progress_callback(progress):
                        self.debug_print("AWSClient: progress_callback returned False. Requesting cancel.")
                        cancel_requested = True
                        # Raise exception immediately to try and stop the transfer
                        raise RuntimeError("Download cancelled by user callback.")
                elif progress_callback: # Handle 0-byte files or files where size wasn't obtained
                     progress_callback(0) # Report 0 progress, but check cancellation
                     if cancel_requested: # Check again in case callback set it
                          raise RuntimeError("Download cancelled by user callback.")

            # Ensure target directory exists
            self.debug_print(f"Ensuring directory exists: {os.path.dirname(save_path)}")
            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            # Configure transfer - use threads=False for simplicity and maybe stability?
            # Max concurrency 1 might help ensure callbacks are more predictable?
            config = boto3.s3.transfer.TransferConfig(
                use_threads=False, # Let's try disabling internal threading first
                # max_concurrency=1 
            )

            self.debug_print(f"Starting boto3 download_file for {key} to {save_path}")
            # Open the file handle *before* starting download for cleanup
            f = open(save_path, 'wb')
            f.close() # Close immediately, boto3 download_file will reopen/write
            
            self.s3_client.download_file(
                Bucket=bucket,
                Key=key,
                Filename=save_path,
                Callback=callback,
                Config=config
            )
            
            # If we reach here without the exception, the download finished before cancellation took effect
            # Or it finished successfully.
            # We rely on the callback raising exception for cancellation.
            self.debug_print("boto3 download_file completed without cancellation exception.")
            # Check final size just in case
            final_size = os.path.getsize(save_path)
            if file_size > 0 and final_size != file_size:
                 self.debug_print(f"Warning: Final file size {final_size} does not match expected size {file_size}.")
                 # Consider this potentially incomplete? For now, return True if no error.
            
            return True

        except RuntimeError as e:
            # Catch our specific cancellation exception
            if str(e) == "Download cancelled by user callback.":
                self.debug_print("AWSClient: Caught cancellation exception. Download stopped.")
                # Cleanup is handled in finally
                return False
            else:
                # Different runtime error
                self.debug_print(f"AWSClient: Download failed with runtime error: {e}")
                # Cleanup handled in finally
                return False
        except Exception as e:
            # Catch other exceptions (ClientError, etc.)
            self.debug_print(f"AWSClient: Download failed with error: {str(e)}")
            # Cleanup handled in finally
            return False
            
        finally:
            self.debug_print("AWSClient: Download function finally block.")
            # Cleanup: Remove the file if cancel was requested or if an error occurred
            # Need to check if 'e' or 'cancel_requested' exist/are true
            error_occurred = 'e' in locals() 
            should_delete = cancel_requested or error_occurred
            
            self.debug_print(f"Cleanup check: cancel_requested={cancel_requested}, error_occurred={error_occurred}, should_delete={should_delete}")

            if should_delete:
                try:
                    if os.path.exists(save_path):
                        self.debug_print(f"Cleanup: Removing potentially partial file: {save_path}")
                        # Ensure file handle 'f' is closed if it was somehow opened and left
                        # (though it shouldn't be in this flow anymore)
                        if f and not f.closed:
                             f.close()
                        os.remove(save_path)
                    else:
                         self.debug_print("Cleanup: File does not exist, no removal needed.")
                except Exception as remove_err:
                    self.debug_print(f"Cleanup: Error removing file: {remove_err}")
            else:
                 self.debug_print("Cleanup: No need to remove file.")
            self.debug_print("AWSClient: Download function finished.")
    
    def delete_object(self, bucket: str, key: str) -> bool:
        """Delete an object from S3."""
        if not self.s3_client:
            raise Exception("Not authenticated")
        
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            print(f"Error deleting object: {str(e)}")
            return False
    
    def get_object_metadata(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        """Get metadata for an S3 object."""
        if not self.s3_client:
            raise Exception("Not authenticated")
        
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            return response
        except ClientError as e:
            print(f"Error getting object metadata: {str(e)}")
            return None
    
    def save_credentials(self, profile_name: str, access_key: str, secret_key: str):
        """Save AWS credentials securely using keyring."""
        try:
            keyring.set_password(
                "s3xplorer",
                f"aws_access_key_{profile_name}",
                access_key
            )
            keyring.set_password(
                "s3xplorer",
                f"aws_secret_key_{profile_name}",
                secret_key
            )
            return True
        except Exception as e:
            print(f"Error saving credentials: {str(e)}")
            return False
    
    def load_credentials(self, profile_name: str) -> Optional[Dict[str, str]]:
        """Load AWS credentials from keyring."""
        try:
            access_key = keyring.get_password(
                "s3xplorer",
                f"aws_access_key_{profile_name}"
            )
            secret_key = keyring.get_password(
                "s3xplorer",
                f"aws_secret_key_{profile_name}"
            )
            
            if access_key and secret_key:
                return {
                    "access_key": access_key,
                    "secret_key": secret_key
                }
            return None
        except Exception as e:
            print(f"Error loading credentials: {str(e)}")
            return None 