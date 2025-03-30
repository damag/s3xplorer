from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                             QLineEdit, QPushButton, QTabWidget, QWidget,
                             QFormLayout, QMessageBox, QComboBox, QProgressBar,
                             QGroupBox)
from PyQt6.QtCore import Qt
from src.core.aws_client import AWSClient
import keyring

class AuthDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.aws_client = AWSClient()
        self.tabs = None  # Store reference to tab widget
        self.setup_ui()
        self.load_saved_fields()
        
        # Connect SSO signals
        self.aws_client.sso_code_ready.connect(self.handle_sso_code)
        self.aws_client.sso_status_update.connect(self.handle_sso_status)
        self.aws_client.sso_completed.connect(self.handle_sso_completed)
    
    def setup_ui(self):
        self.setWindowTitle("AWS Authentication")
        self.setMinimumWidth(400)
        
        layout = QVBoxLayout(self)
        
        # Create tab widget for different auth methods
        self.tabs = QTabWidget()  # Store reference
        
        # SSO tab
        sso_tab = QWidget()
        sso_layout = QVBoxLayout(sso_tab)  # Changed to VBoxLayout for better organization
        
        # Add authorization code display at the top
        code_group = QGroupBox("Authorization Code")
        code_layout = QVBoxLayout()
        self.sso_code_label = QLabel()
        self.sso_code_label.setStyleSheet("""
            font-size: 24px;
            font-weight: bold;
            color: #2196F3;
            padding: 10px;
            background-color: #E3F2FD;
            border: 2px solid #2196F3;
            border-radius: 5px;
        """)
        self.sso_code_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.sso_code_label.hide()
        code_layout.addWidget(self.sso_code_label)
        code_group.setLayout(code_layout)
        sso_layout.addWidget(code_group)
        
        # Add form layout for SSO inputs
        sso_form = QFormLayout()
        self.start_url_input = QLineEdit()
        self.sso_region_input = QLineEdit("eu-west-1")  # Set default region
        self.account_id_input = QLineEdit()
        self.role_name_input = QLineEdit()
        
        sso_form.addRow("Start URL:", self.start_url_input)
        sso_form.addRow("Region:", self.sso_region_input)
        sso_form.addRow("Account ID:", self.account_id_input)
        sso_form.addRow("Role Name:", self.role_name_input)
        
        sso_layout.addLayout(sso_form)
        
        # Access Key tab
        access_key_tab = QWidget()
        access_key_layout = QFormLayout(access_key_tab)
        
        self.access_key_input = QLineEdit()
        self.secret_key_input = QLineEdit()
        self.secret_key_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.region_input = QLineEdit("us-east-1")
        
        access_key_layout.addRow("Access Key:", self.access_key_input)
        access_key_layout.addRow("Secret Key:", self.secret_key_input)
        access_key_layout.addRow("Region:", self.region_input)
        
        # Profile tab
        profile_tab = QWidget()
        profile_layout = QFormLayout(profile_tab)
        
        self.profile_input = QLineEdit()
        profile_layout.addRow("Profile Name:", self.profile_input)
        
        # Add tabs
        self.tabs.addTab(sso_tab, "SSO")
        self.tabs.addTab(access_key_tab, "Access Key")
        self.tabs.addTab(profile_tab, "Profile")
        
        layout.addWidget(self.tabs)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        self.connect_button = QPushButton("Connect")
        self.connect_button.clicked.connect(self.handle_connect)
        
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)
        
        button_layout.addWidget(self.connect_button)
        button_layout.addWidget(self.cancel_button)
        
        layout.addLayout(button_layout)
    
    def load_saved_fields(self):
        """Load saved authentication fields from keyring."""
        try:
            # Load access key fields
            access_key = keyring.get_password("s3xplorer", "aws_access_key")
            secret_key = keyring.get_password("s3xplorer", "aws_secret_key")
            region = keyring.get_password("s3xplorer", "aws_region")
            
            if access_key:
                self.access_key_input.setText(access_key)
            if secret_key:
                self.secret_key_input.setText(secret_key)
            if region:
                self.region_input.setText(region)
            
            # Load profile
            profile = keyring.get_password("s3xplorer", "aws_profile")
            if profile:
                self.profile_input.setText(profile)
            
            # Load SSO fields
            start_url = keyring.get_password("s3xplorer", "aws_sso_start_url")
            sso_region = keyring.get_password("s3xplorer", "aws_sso_region")
            account_id = keyring.get_password("s3xplorer", "aws_sso_account_id")
            role_name = keyring.get_password("s3xplorer", "aws_sso_role_name")
            
            if start_url:
                self.start_url_input.setText(start_url)
            if sso_region:
                self.sso_region_input.setText(sso_region)
            if account_id:
                self.account_id_input.setText(account_id)
            if role_name:
                self.role_name_input.setText(role_name)
                
        except Exception as e:
            print(f"Error loading saved fields: {str(e)}")
    
    def save_fields(self):
        """Save authentication fields to keyring."""
        try:
            # Save access key fields
            keyring.set_password("s3xplorer", "aws_access_key", self.access_key_input.text())
            keyring.set_password("s3xplorer", "aws_secret_key", self.secret_key_input.text())
            keyring.set_password("s3xplorer", "aws_region", self.region_input.text())
            
            # Save profile
            keyring.set_password("s3xplorer", "aws_profile", self.profile_input.text())
            
            # Save SSO fields
            keyring.set_password("s3xplorer", "aws_sso_start_url", self.start_url_input.text())
            keyring.set_password("s3xplorer", "aws_sso_region", self.sso_region_input.text())
            keyring.set_password("s3xplorer", "aws_sso_account_id", self.account_id_input.text())
            keyring.set_password("s3xplorer", "aws_sso_role_name", self.role_name_input.text())
            
        except Exception as e:
            print(f"Error saving fields: {str(e)}")
    
    def handle_sso_code(self, code: str):
        """Handle the SSO authorization code."""
        print(f"Received authorization code: {code}")  # Debug print
        self.sso_code_label.setText(code)
        self.sso_code_label.show()
        self.sso_code_label.setStyleSheet("font-size: 16px; font-weight: bold; color: #2196F3;")
        # Force the dialog to update
        self.sso_code_label.update()
        self.update()
    
    def handle_sso_status(self, status: str):
        """Handle SSO status updates."""
        if "failed" in status.lower() or "timed out" in status.lower():
            self.connect_button.setEnabled(True)
            self.cancel_button.setEnabled(True)
        else:
            self.connect_button.setEnabled(False)
            self.cancel_button.setEnabled(False)
    
    def handle_sso_completed(self, success: bool):
        """Handle SSO authentication completion."""
        if success:
            self.accept()
        else:
            self.connect_button.setEnabled(True)
            self.cancel_button.setEnabled(True)
    
    def handle_connect(self):
        print("handle_connect called")  # Debug print
        current_tab = self.tabs.currentIndex()
        print(f"Current tab: {current_tab}")  # Debug print
        
        try:
            if current_tab == 0:  # SSO tab
                print("Starting SSO authentication...")  # Debug print
                region = self.sso_region_input.text().strip()
                if not region:
                    QMessageBox.warning(
                        self,
                        "Error",
                        "Please specify a region for SSO authentication."
                    )
                    return
                
                # Validate and format account ID
                account_id = self.account_id_input.text().strip()
                if not account_id:
                    QMessageBox.warning(
                        self,
                        "Error",
                        "Please specify an Account ID for SSO authentication."
                    )
                    return
                
                # Remove any non-digit characters from account ID
                account_id = ''.join(filter(str.isdigit, account_id))
                if not account_id or len(account_id) != 12:
                    QMessageBox.warning(
                        self,
                        "Error",
                        "Invalid Account ID format. Please enter a valid 12-digit AWS account number."
                    )
                    return
                
                # Validate role name
                role_name = self.role_name_input.text().strip()
                if not role_name:
                    QMessageBox.warning(
                        self,
                        "Error",
                        "Please specify a Role Name for SSO authentication."
                    )
                    return
                
                # Validate role name format (should be alphanumeric with hyphens and underscores)
                if not all(c.isalnum() or c in '-_' for c in role_name):
                    QMessageBox.warning(
                        self,
                        "Error",
                        "Invalid Role Name format. Role names can only contain alphanumeric characters, hyphens, and underscores."
                    )
                    return
                
                # Validate start URL
                start_url = self.start_url_input.text().strip()
                if not start_url:
                    QMessageBox.warning(
                        self,
                        "Error",
                        "Please specify a Start URL for SSO authentication."
                    )
                    return
                
                if not start_url.startswith('https://'):
                    QMessageBox.warning(
                        self,
                        "Error",
                        "Start URL must begin with 'https://'"
                    )
                    return
                
                print(f"SSO parameters validated: region={region}, account_id={account_id}, role_name={role_name}, start_url={start_url}")  # Debug print
                
                # Disable UI during SSO process
                self.connect_button.setEnabled(False)
                self.cancel_button.setEnabled(False)
                
                try:
                    print("Calling authenticate_with_sso...")  # Debug print
                    success = self.aws_client.authenticate_with_sso(
                        start_url,
                        region,
                        account_id,
                        role_name
                    )
                    print(f"authenticate_with_sso returned: {success}")  # Debug print
                    if success:
                        self.save_fields()
                    else:
                        QMessageBox.warning(
                            self,
                            "Error",
                            "Failed to connect to AWS. Please check your credentials."
                        )
                except Exception as e:
                    print(f"Exception in authenticate_with_sso: {str(e)}")  # Debug print
                    error_msg = str(e)
                    if "ForbiddenException" in error_msg:
                        QMessageBox.critical(
                            self,
                            "Access Denied",
                            "Unable to access the specified role. Please check:\n\n"
                            "1. The Role Name is correct\n"
                            "2. The Account ID is correct\n"
                            "3. You have permission to assume this role\n"
                            "4. The role exists in the specified account"
                        )
                    else:
                        raise
            elif current_tab == 1:  # Access Key tab
                success = self.aws_client.authenticate_with_access_key(
                    self.access_key_input.text(),
                    self.secret_key_input.text(),
                    self.region_input.text()
                )
                if success:
                    self.save_fields()
                    self.accept()
                else:
                    QMessageBox.warning(
                        self,
                        "Error",
                        "Failed to connect to AWS. Please check your credentials."
                    )
            else:  # Profile tab
                success = self.aws_client.authenticate_with_profile(
                    self.profile_input.text()
                )
                if success:
                    self.save_fields()
                    self.accept()
                else:
                    QMessageBox.warning(
                        self,
                        "Error",
                        "Failed to connect to AWS. Please check your credentials."
                    )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                f"An error occurred: {str(e)}"
            )
            # Re-enable UI in case of error
            self.connect_button.setEnabled(True)
            self.cancel_button.setEnabled(True)
    
    def get_aws_client(self):
        return self.aws_client 