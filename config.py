# config.py

# MongoDB Configuration
LOCAL_MONGO_URI = "mongodb://localhost:27017/"
SERVER_MONGO_URI = "mongodb://mysql-srv:27017/"
DB_NAME = "clinical_trials"
COLLECTION_METADATA = "metadata"
COLLECTION_PROJECTS = "projects"
# TODO: Change to a valid Linux directory
BACKUP_DIR = r"C:\dev\mongo_backups"  # Backup storage directory