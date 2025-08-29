```bash
# Start just the database
docker-compose -f docker-compose.yml up -d

# Check it's running
docker-compose -f docker-compose.yml ps
```

```bash

# Run for credit cards
uv run python main.py data/csv_files/credit_cards --account-type credit_card

# Run for bank accounts
uv run python main.py data/csv_files/bank_accounts --account-type bank_account

# Query db
 docker-compose exec db psql -U postgres -d finance -c "SELECT * FROM bank_account_transactions WHERE amount > 1000"                                                              
```

