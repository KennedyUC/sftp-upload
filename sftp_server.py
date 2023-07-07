# Import the necessary libraries
import pysftp
import logging
import configparser

# Load the parameters from the config file
config = configparser.ConfigParser()
config.read('config.cfg.template', encoding='utf-8-sig')
vm_ext_ip              =  config['GCP']['HOST_NAME']
vm_user                =  config['GCP']['USERNAME']
vm_user_pass           =  config['GCP']['PASSWORD']
gcp_credential_path    =  config['GCP']['CREDENTIAL_PATH']
gcp_bucket_name        =  config['GCP']['BUCKET_NAME']
gcp_project_name       =  config['GCP']['PROJECT_ID']


class SFTP_Server():
    """
    This class is used to initialize the SFTP server and start the server connection"""
    def __init__(self, host_name, user_name, pass_word):
        self.host_name = host_name
        self.user_name = user_name
        self.pass_word = pass_word
        
    def connect(self):
        """
        This class method is used to connect to the SFTP server
        """
        
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            sftp = pysftp.Connection(host=self.host_name, username=self.user_name, password=self.pass_word, cnopts=cnopts)
            return sftp
    
        except Exception:
            logging.exception("An error occured. Check the credentials")    


if __name__ == '__main__':
    # Intantiate the remote server
    server = SFTP_Server(vm_ext_ip, vm_user, vm_user_pass)

    # Connect to the remote server
    sftp = server.connect()

    # List the files in the present working directory in the SFTP server
    work_dir = sftp.pwd
    file_list = sftp.listdir(work_dir)

    print(f'Present working directory in the server: {work_dir}')
    print(f'List of files in {work_dir}: {file_list}')

    # Close the SFTP server connection
    sftp.close()
    print('Server closed')