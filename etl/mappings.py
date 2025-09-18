# Column mappings from raw csv files to db schema
def get_chase_credit_card_mapping():
    """Column mapping for Raw Chase Credit Card CSV Data"""
    return {
        "Transaction Date": "date",
        "Post Date": "post_date",
        "Description": "description",
        "Category": "category",
        "Type": "transaction_type",
        "Amount": "amount",
        "Memo": "memo",
    }


def get_chase_bank_account_mapping():
    """Column mapping for Raw Chase Bank Account CSV Data"""
    return {
        "Details": "details",
        "Posting Date": "date",
        "Description": "description",
        "Amount": "amount",
        "Type": "type",
        "Balance": "balance",
        "Check or Slip #": "check_number",
    }