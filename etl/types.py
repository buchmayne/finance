from enum import Enum
from dataclasses import dataclass
from pathlib import Path

class AccountType(Enum):
    BANK_ACCOUNT : str = "bank_account"
    CREDIT_CARD : str = "credit_card"

@dataclass
class PathCSVDirectories:
    project_root : Path = Path(__file__).parent.parent
    base_dir : Path = project_root / "data" / "csv_files"
    bank_accounts_dir : Path = base_dir / "bank_accounts"
    credit_cards_dir : Path = base_dir / "credit_cards"

