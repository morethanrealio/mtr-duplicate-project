import os
from dotenv import load_dotenv

load_dotenv()

SSH_CONFIG = {
    'ssh_address': os.getenv('SSH_ADDRESS'),
    'ssh_port': int(os.getenv('SSH_PORT')),
    'ssh_username': os.getenv('SSH_USERNAME'),
    'ssh_private_key': os.getenv('SSH_PRIVATE_KEY')
}

POSTGRES_CONFIG = {
    'db_name': os.getenv('POSTGRES_DB_NAME'),
    'db_user': os.getenv('POSTGRES_DB_USER'),
    'db_password': os.getenv('POSTGRES_DB_PASSWORD'),
    'db_host': os.getenv('POSTGRES_DB_HOST'),
    'db_port': int(os.getenv('POSTGRES_DB_PORT'))
}
