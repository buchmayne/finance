import re
import pandas as pd
import hashlib
from pathlib import Path
from datetime import datetime
from models import BankAccountTransaction, CreditCardTransaction, get_db, create_tables

# Column mappings
def get_chase_credit_card_mapping():
    """Column mapping for Chase Credit Card CSV"""
    return {
        'Transaction Date': 'date',
        'Post Date': 'post_date',
        'Description': 'description', 
        'Category': 'chase_category',
        'Type': 'transaction_type',
        'Amount': 'amount',
        'Memo': 'memo'
        
    }

def get_chase_bank_account_mapping():
    """Column mapping for Chase Bank Account CSV"""
    return {
        'Details': 'details',
        'Posting Date': 'date',
        'Description': 'description',
        'Amount': 'amount',
        'Type': 'type',
        'Balance': 'balance',
        'Check or Slip #': 'check_number'
    }


class SimpleCSVImporter:
    def __init__(self):
        self.db = get_db()
    
    def preview_csv(self, file_path: str) -> pd.DataFrame:
        """Preview CSV file structure"""
        df = pd.read_csv(file_path, nrows=5)
        
        print(f"\n📄 File: {Path(file_path).name}")
        print(f"📊 Shape: {df.shape}")
        print(f"📝 Columns: {list(df.columns)}")
        print("\n🔍 Sample data:")
        print(df.to_string())
        return df
    
    def _generate_id(self, row) -> str:
        """Generate unique ID for transaction"""
        # Create hash from date + amount + description
        content = f"{row['date']}{row['amount']}{str(row['description'])[:50]}"
        return hashlib.md5(content.encode()).hexdigest()[:12]
    
    def _extract_chase_account(self, file_path: str) -> str | None:
        patterns = [
            r'Chase\s*\d{4}',
            r'Chase.*?ending\s+in\s+(\d{4})',
            r'Chase.*?(\d{4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, file_path, re.IGNORECASE)
            if match:
                return re.sub(r'\s+', '', match.group())
        
        return None
    
    def close(self):
        """Close database connection"""
        if self.db:
            self.db.close()

class BankAccountCSVImporter(SimpleCSVImporter):
    
    def import_csv(self, file_path: str, column_mapping: dict, dry_run: bool = True) -> pd.DataFrame:
        """Import CSV with column mapping"""
        
        # Read CSV
        df = pd.read_csv(file_path, usecols=range(1, 7)) # first column is NULL Details
        print(f"📥 Loaded {len(df)} rows from {Path(file_path).name}")
        
        # Apply column mapping
        df = df.rename(columns=column_mapping)
        
        # Basic data cleaning
        df = self._clean_data(df)

        # Extract account number
        account_number = self._extract_chase_account(file_path)
        
        if dry_run:
            print("🏃 DRY RUN - No data will be saved")
            print(f"Would import {len(df)} transactions:")
            print(df[['date', 'amount', 'description']].head(10).to_string())
            return df
        
        # Save to database
        saved_count = 0
        for _, row in df.iterrows():
            transaction = BankAccountTransaction(
                id=self._generate_id(row),
                date=row['date'],
                amount=row['amount'],
                description=row['description'],
                balance=row['balance'],
                account=account_number,
                source_file=Path(file_path).name
            )
            
            # Simple upsert logic
            existing = self.db.query(BankAccountTransaction).filter(BankAccountTransaction.id == transaction.id).first()
            if not existing:
                self.db.add(transaction)
                saved_count += 1
        
        self.db.commit()
        print(f"✅ Imported {saved_count} new transactions")
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic data cleaning"""
        
        # Parse dates
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        # Clean amounts (remove $ signs, commas)
        if 'amount' in df.columns:
            df['amount'] = df['amount'].astype(str).str.replace(r'[\$,]', '', regex=True)
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        # Fill missing descriptions
        if 'description' in df.columns:
            df['description'] = df['description'].fillna('No description')
        
        return df

class CreditCardCSVImporter(SimpleCSVImporter):
    
    def import_csv(self, file_path: str, column_mapping: dict, dry_run: bool = True) -> pd.DataFrame:
        """Import CSV with column mapping"""
        
        # Read CSV
        df = pd.read_csv(file_path)
        print(f"📥 Loaded {len(df)} rows from {Path(file_path).name}")
        
        # Apply column mapping
        df = df.rename(columns=column_mapping)
        
        # Basic data cleaning
        df = self._clean_data(df)

        # Extract card number
        card_number = self._extract_chase_account(file_path)
        
        if dry_run:
            print("🏃 DRY RUN - No data will be saved")
            print(f"Would import {len(df)} transactions:")
            print(df[['date', 'amount', 'description']].head(10).to_string())
            return df
        
        # Save to database
        saved_count = 0
        for _, row in df.iterrows():
            transaction = CreditCardTransaction(
                id=self._generate_id(row),
                date=row['date'],
                amount=row['amount'],
                description=row['description'],
                chase_category=row.get('chase_category', 'Other'),
                card_number=card_number,
                source_file=Path(file_path).name
            )
            
            # Simple upsert logic
            existing = self.db.query(CreditCardTransaction).filter(CreditCardTransaction.id == transaction.id).first()
            if not existing:
                self.db.add(transaction)
                saved_count += 1
        
        self.db.commit()
        print(f"✅ Imported {saved_count} new transactions")
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic data cleaning"""
        
        # Parse dates
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        # Clean amounts (remove $ signs, commas)
        if 'amount' in df.columns:
            df['amount'] = df['amount'].astype(str).str.replace(r'[\$,]', '', regex=True)
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        # Fill missing descriptions
        if 'description' in df.columns:
            df['description'] = df['description'].fillna('No description')
        
        # Add basic category if missing
        if 'chase_category' not in df.columns:
            df['chase_category'] = 'Other'
        
        return df


