from etl.layers import raw, staging

class ETLPipeline:
    def __init__(self):
        self.layers = {
            'raw': [raw.import_bank_accounts, raw.import_credit_cards],
            'staging': [staging.create_staging_bank_account_transactions, staging.create_staging_credit_card_transactions], 
            # 'marts': [marts.create_categorized_transactions],
            # 'metrics': [metrics.create_monthly_spending_summary]
        }
    
    def run_layer(self, layer_name: str):
        """Run all transformations in a specific layer"""
        if layer_name not in self.layers:
            raise ValueError(f"Unknown layer: {layer_name}")
            
        for transform_func in self.layers[layer_name]:
            print(f"Running {transform_func.__name__}...")
            transform_func()
    
    def run_full_pipeline(self):
        """Run entire pipeline in dependency order"""
        # for layer_name in ['raw', 'staging', 'marts', 'metrics']:
        for layer_name in ['raw', 'staging']:
            self.run_layer(layer_name)