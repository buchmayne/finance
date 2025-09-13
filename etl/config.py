import os
from pathlib import Path

class Settings:
    project_root = Path(__file__).parent.parent.parent
    db_path = project_root / "finance.db"
    database_url = f"sqlite:///{db_path}"
    
settings = Settings()