import pandas as pd
import numpy as np
from etl.database import get_db


def _normalize_description(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize text formatting for transaction descriptions"""
    return (
        df
        .assign(
            description=(
                df['description']
                .str.upper()
                .str.replace(r'\s+', ' ', regex=True)
                .str.strip()
            )
        )
    )

def _normalize_date(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize transaction date and add columns for transaction year and month"""
    return (
        df
        .assign(
            date=(
                pd.to_datetime(df['date']).dt.date
            ),
            year=pd.to_datetime(df['date']).dt.year,
            month=pd.to_datetime(df['date']).dt.month,
            day_of_week=pd.to_datetime(df['date']).dt.weekday + 1, # Monday = 1 & Sunday = 7 more intuitive
        )
        .assign(
            year_month=lambda df_: df_['year'].astype(str) + '-' + df_['month'].astype(str).map({str(x): f"0{x}" if len(str(x)) == 1 else str(x) for x in list(range(1, 13))})
        )
    )


# Categorize Bank Account Transactions
def _categorize_individual_bank_transaction(description: str) -> str:
    """Logic to categorize transactions based on description"""
    salary_desc = [
        'CLEARCOVER INC PAYROLL',
        'FEDEX DATAWORKS DIR DEP',
        'ECONOMIC CONSULT PAYROLL',
        'EMPLOYMT BENEFIT UI BENEFIT PPD'
    ]

    account_transfers_desc = [
        'ONLINE TRANSFER TO SAV',
        'ONLINE TRANSFER TO CHK',
        'ONLINE TRANSFER FROM SAV',
        'ONLINE TRANSFER FROM CHK',
    ]

    jenna_wedding_acct_desc = [
        'WESTFIELD BANK ACCTVERIFY',
        'WESTBK CK WEBXFR P2P JENNA CARLSON'
    ]

    if any(pattern in description for pattern in salary_desc):
        return 'SALARY'
    elif description == 'INTEREST PAYMENT':
        return 'ACCOUNT_INTEREST'
    elif 'VERIZON WIRELESS PAYMENTS' in description:
        return 'CELL_PHONE_BILL'
    elif 'VANGUARD BUY INVESTMENT' in description:
        return 'TRANSFER_TO_BROKERAGE'
    elif any(pattern in description for pattern in ['VANGUARD SELL INVESTMENT', 'APA TREAS 310 MISC PAY PPD']):
        return 'TRANSFER_FROM_BROKERAGE'
    elif 'VENMO PAYMENT' in description:
        return 'VENMO_PAYMENT'
    elif 'VENMO CASHOUT' in description:
        return 'VENMO_CASHOUT'
    elif 'PINNACLE COA' in description:
        return 'HOA_PAYMENT'
    elif any(pattern in description for pattern in ['CHASE CREDIT CRD AUTOPAY', 'PAYMENT TO CHASE CARD']):
        return 'CREDIT_CARD_PAYMENT'
    elif any(pattern in description for pattern in ['ONPOINT COMMUNIT RE PAYMENT', 'ONPOINT COMM CU MTG PYMTS']):
        return 'MORTGAGE_PAYMENT'
    elif any(pattern in description for pattern in account_transfers_desc):
        return 'TRANSFER_BETWEEN_CHASE_ACCOUNTS'
    elif any(pattern in description for pattern in ['DEPOSIT ID NUMBER', 'REMOTE ONLINE DEPOSIT']):
        return 'CASH_DEPOSIT'
    elif description == 'WITHDRAWAL 07/14':
        return 'CASH_WITHDRAWL_FOR_WEDDING'
    elif 'WITHDRAWAL' in description:
        return 'CASH_WITHDRAWL'
    elif 'ALEX ELISE' in description:
        return 'WEDDING_PHOTOGRAPHER'
    elif 'WEX HEALTH PREMIUMS 28670940 WEB ID' in description:
        return 'COBRA_PAYMENTS'
    elif any(pattern in description for pattern in ['OR REVENUE DEPT ORSTTAXRFD', 'IRS TREAS 310 TAX REF']):
        return 'TAX_REFUND'
    elif 'CHECK # 1976 PASSPORTSERVICES PAYMENT ARC ID' in description:
        return 'PASSPORT_RENEWAL'
    elif any(pattern in description for pattern in jenna_wedding_acct_desc):
        return 'JENNA_WEDDING_ACCT_TRANSFERS'
    else:
        return 'OTHER'


def _categorize_all_bank_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Apply transactions categorization to all transactions"""
    return (
        df
        .assign(category=df['description'].apply(_categorize_individual_bank_transaction))
    )

# Categorize Credut Card Transactions
def _categorize_individual_credit_card_transaction(description: str) -> str:
    """Logic to categorize transactions based on description"""
    def _match(desc : list, description: str = description) -> bool:
        return any(pattern in description for pattern in desc)
    
    cc_payment_desc = [
        'PAYMENT THANK YOU-MOBILE',
        'AUTOMATIC PAYMENT - THANK',
        'PAYMENT THANK YOU - WEB'
    ]

    coffee_desc = [
        'SQ *COFFEE TIME',
        'CAFFE UMBRIA PORTLAND',
        'SQ *SISTERS COFFEE COMPAN',
        'TST*CAFFE UMBRIA PORTLAN',
        'GOOD COFFEE'
    ]

    lunch_desc = [
        'CHIPOTLE ONLINE',
        'TST*PIZZICATO - PEARL',
        'TST* PIZZICATO - PEARL',
        'SQ *LOVEJOY BAKERS',
        'CHIPOTLE MEX GR ONLINE',
        'CHIPOTLE 1358',
        'TST* Pizzicato - Pearl'
    ]

    concerts_desc = [
        'HAWTHORNE THEATER',
        'TST*REVOLUTION HALL',
        'TST* MCMENAMINS - CRYSTAL',
        'CASCADES AMPHITHEATRE',
        'SEATGEEK TICKETS',
        'AXS.COMFESTIVAL GV R',
        'TM *KAYTRANADA X JUSTI',
        'MCMENAMINS CONCERTS',
        'CASCADE TICKETS',
        'REVOLUTION HALL',
        'TCKTWEB*GOATWHOREVITRI',
        'PP*GATES TO HELL',
        'SQ *VITRIOL'
    ]

    nbhd_bars_desc = [
        'PAYMASTER LOUNGE',
        "TST* JERRY'S TAVERN",
        'JOES CELLAR',
        "JOE'S CELLAR",
        'SPO*THEFIELDSBAR&amp;GRILL',
        'THE FIELDS BAR &AMP; GRILL',
        'CARLITAS',
        'THEFIELDSBAR'
    ]

    groceries_desc = [
        'SAFEWAY #2790',
        'NEW SEASONS MARKET',
        'WHOLEFDS PRT 10148',
        'FRED-MEYER #0360',
        "ZUPAN'S MARKET",
        'COSTCO WHSE #0111',
        'ALBERTSONS #3531',
        'WHOLEFDS BRD 10266',
        'WHOLE FOODS PRT 10148',
        'UWAJIMAYA',
        'TRADER JOE S #146',
        'THE MEATING PLACE',
        'WORLD FOODS',
        'COSTCO WHSE #0780',
        'CVS/PHARMACY #11282'
    ]

    movies_desc = [
        'REGAL CINEMAS INC',
        'CINEMA 21',
        'FOX TOWER STM 10',
        'HOLLYWOOD THEATRE',
        'LIVING ROOM THEATERS',
        'REGAL BRIDGEPORT  0652'
    ]

    wedding_desc = [
        'BLACK BUTTE RANCH (1)',
        'ZOLA.COM*REGISTRY',
        'BLACK BUTTE RANCH FOOD',
        'IN *THE BOB LLC',
        'FORYOURPARTY',
        'SISTERS SALOON &AMP; RANCH',
        'PROPER CLOTH',
        'MORJAS',
        'EUROPEAN MASTER TAILOR',
    ]

    fast_food_desc = [
        'SQ *SHAKE SHACK',
        "MCDONALD'S",
        'BURGERVILLE',
        'JACK IN THE BOX 7160'
    ]

    eating_out_desc = [
        'SQ *GASTRO MANIA',
        'JOJO PEARL',
        'TST* WILD CHILD PIZZA - F',
        'MOMO YAMA',
        'TST* SIZZLE PIE - WEST',
        'TST* MISSISSIPPI STUDIOS',
        'TST* 10 BARREL BREWING -',
        "TST* SCOTTIE'S PIZZA PARL",
        'TST* BREAKSIDE BREWERY -',
        'TST* 10 BARREL PORTLAND N',
        'TST* QDS',
        'TST* SILVER HARBOR BREWIN',
        'TST*RIVER PIG - PORTLAND',
        'YAMA SUSHI AND SAKE BAR',
        'BANNINGS RESTAURANT &amp; PIE',
        'TST* GARDEN TAVERN',
        'TST* FIRE ON THE MOUNTAIN',
        'THE TRIPLE LINDY',
        "SQ *SCOTTIE'S PIZZA PARLO",
        'SQ *RANCH PIZZA SOUTHEAST ',
        'PROST TAVERN PORTLAND',
        'RINGSIDE STEAK HOUSE WEST',
        'SQ *GROUND KONTROL CLASSI',
        'SQ *RANCH PIZZA SOUTHEAST',
        'SQ *BAERLIC SOUTHEAST',
        'LOYAL LEGION',
        'MARATHON TAVERNA',
        'OX',
        'LUCKY LABRADOR BEER HALL',
        'K-TOWN KOREAN BBQ',
        'PORTLAND CITY GRILL-PO',
        'Hale Pele',
        'SQ *UPRIGHT BREWING',
        'SQ *FREELAND SPIRITS',
        'SQ *JOHNS MARKETPLACE',
        '9TH AVE MINI MART',
        'ORGEATWORKS',
        'AP MARKET',
        'DIVISION FOOD MART PDX',
        'ALBERTA STREET MARKET',
        'SQ *UP NORTH SURF CLUB',
        'RAYS FOOD PLACE #45',
        '50TH MARKET ',
        'GROUND KONTROL CLASSIC AR',
        'KINGPINS - BEAVERTON - BO',
        'BANNINGS RESTAURANT',
        'RANCH PIZZA'
    ]

    clothing_desc = [
        'NORDSTROM',
        'FJAELLRAEVEN',
        'ON INC',
        'TOMMY BAHAMA 613',
        'WARBY PARKER',
        'BONOBOS',
        'VINTAGE SPORTS FASHION',
        'SP WADE AND WILLIAMS',
        'SP ANDAFTERTHAT',
        'NORDSTROM #0025'
    ]

    physical_media_desc = [
        'EVERYDAY MUSIC',
        'CRITERION.COM',
        'BARNES&AMP;NOBLE PAPERSOURCE',
        'BARNES &AMP; NOBLE 2371',
        'MUSIC MILLENNIUM'
        'ARROW FILMS',
    ]

    hotels_desc = [
        'WARWICK ALLERTON HOTEL',
        'HOOD RIVER HOTEL',
        'AIRBNB * HMPSDMXX99',
        'COURTYARD BY MARRIOTT',
        'MARRIOTT SN FRAN MARQU',
        'BEST WESTERN PONDEROSA',
        'HILTON'
    ]

    flights_desc = [
        'ALASKA AIR',
        'UNITED ',
        'AMERICAN AIR'
    ]

    gifts_desc = [
        'PENDLETON',
        'LULULEMON BRIDGEPORT',
        'HONEYFUND.COMGIFTCARDS',
        'SP KIRIKO',
        'SP BABYLIST',
        'SP WWW.POSHBABY.COM',
        'SQ *VIK ROASTERS',
        'SP ECRU MODERN STATI',
        'LS OBLATIONPAPERS.COM'
    ]

    home_improvement_desc = [
        'PEARL HARDWARE',
        'CRATE &AMP; BARREL #454',
        'RESTORATION HARDWARE',
        'THE HOME DEPOT 4002',
        'WILLIAMS-SONOMA 6324',
        'KITCHEN KABOODLE'
    ]

    car_maintenance_desc = [
        'LES SCHWAB TIRES #0243',
        'ODOT DMV2U',
        'DEQ VIP DEQ TOO'
    ]

    ai_desc = [
        'CLAUDE.AI SUBSCRIPTION',
        'CHATGPT SUBSCRIPTION',
        'OPENAI'
    ]

    software_desc = [
        'DNH*DOMAINS#3405924658',
        'GOOGLE *Domains',
        'AMAZON WEB SERVICES',
        'DIGITALOCEAN.COM'
    ]

    gas_desc = [
        'ASTRO',
        'SHELL',
        '76',
        'CHEVRON'
    ]

    if _match(cc_payment_desc):
        return 'CREDIT_CARD_PAYMENT'
    elif 'SQ *OVATION COFFEE' in description:
        return 'OVATION'
    elif _match(coffee_desc):
        return 'OTHER_COFFEE_SHOPS'
    elif _match(lunch_desc):
        return 'EATING_OUT_NBHD_LUNCH'
    elif 'DOMINO' in description:
        return 'DOMINOS'
    elif _match(concerts_desc):
        return 'CONCERTS'
    elif _match(nbhd_bars_desc):
        return 'NBHD_BARS'
    elif 'LYFT' in description or 'UBER' in description:
        return 'RIDESHARE'
    elif _match(groceries_desc):
        return 'GROCERIES'
    elif 'LIQUOR STORE' in description or 'ROLLING RIVER SPIRITS' in description:
        return 'LIQUOR_STORE'
    elif description == 'PORTLAND GENERAL ELECTRIC':
        return 'PGE'
    elif 'LA FIT' in description:
        return 'GYM_MEMBERSHIP'
    elif 'SPOTIFY' in description:
        return 'SPOTIFY_MEMBERSHIP'
    elif 'COMCAST' in description:
        return 'COMCAST'
    elif 'SQ *MICHELLE THRASHER' in description or 'SQ *SLABTOWN BARBERSHOP' in description:
        return 'HAIRCUT'
    elif description == "POWELL'S BURNSIDE":
        return 'POWELLS'
    elif _match(movies_desc):
        return 'MOVIES'
    elif _match(wedding_desc):
        return 'WEDDING'
    elif _match(fast_food_desc):
        return 'FAST_FOOD'
    elif _match(eating_out_desc):
        return 'EATING_OUT'
    elif _match(clothing_desc):
        return 'CLOTHES'
    elif 'ARSENAL' in description:
        return 'ARSENAL'
    elif _match(physical_media_desc):
        return 'PHYSICAL_MEDIA'
    elif description == 'APPLE.COM/BILL':
        return 'APPLE_CLOUD_STORAGE'
    elif _match(hotels_desc):
        return 'TRAVEL_LODGING'
    elif _match(flights_desc):
        return 'FLIGHTS'
    elif 'PARKING' in description:
        return 'PARKING'
    elif 'MODA CENTER' in description:
        return 'MODA_CENTER'
    elif description == 'ROKU FOR WARNERMEDIA GLOB':
        return 'HBO_SUBSCRIPTION'
    elif 'PORTLAND INDOOR SOCCE' in description:
        return 'INDOOR_SOCCER'
    elif _match(gifts_desc):
        return 'GIFTS'
    elif _match(home_improvement_desc):
        return 'HOME_IMPROVEMENT'
    elif 'GORGE PERFORMANCE' in description or 'SP TRAVELERSURFCLUB' in description:
        return 'SURFING'
    elif 'XBOX' in description or 'PLAYSTATION' in description:
        return 'VIDEO_GAMES'
    elif 'WILLAMETTE DRY' in description:
        return 'DRY_CLEANING'
    elif 'PRIME VIDEO' in description or 'GOOGLE *TV' in description:
        return 'VOD_AMAZON'
    elif 'GEICO' in description:
        return 'CAR_INSURANCE'
    elif _match(car_maintenance_desc):
        return 'CAR_MAINTENANCE'
    elif _match(software_desc):
        return 'HOSTING_SOFTWARE_PROJECTS'
    elif _match(ai_desc):
        return 'AI_SUBSCRIPTION'
    elif 'AMAZON PRIME' in description:
        return 'AMAZON_PRIME'
    elif 'AMAZON' in description or 'AMZN' in description:
        return 'AMAZON_PURCHASE'
    elif 'CHESS.COM' in description:
        return 'CHESS_SUBSCRIPTION'
    elif 'TCGPLAYER' in description or 'MAKEPLAYINGCARDS' in description:
        return 'MTG'
    elif description == 'ROKU FOR PEACOCK TV LLC':
        return 'PEACOCK_SUBSCRIPTION'
    elif 'GOOGLE *PARAMOUNT' in description or 'CBS MOBILE APP' in description:
        return 'PARAMOUNT_SUBSCRIPTION'
    elif _match(gas_desc):
        return 'GAS'
    elif description == 'HRB ONLINE TAX PRODUCT':
        return 'FILING_TAXES'
    elif description == 'JEWELERS-MUTUAL-PMNT':
        return 'DIAMOND_INSURANCE'
    elif description == 'BLAZERVISION':
        return 'BLAZER_VISION_SUBSCRIPTION'
    elif description == 'DUNCD ON PRIME':
        return 'PODCAST_SUBSCRIPTION'
    elif 'ARTS TAX' in description:
        return 'PORTLAND_ARTS_TAX'
    elif 'OPAL CAMERA' in description:
        return 'COMPUTERS_TECHNOLOGY_HARDWARE'
    elif 'USPS PO' in description or 'FEDEX OFFIC' in description:
        return 'SHIPPING'
    elif 'RODEO' in description:
        return 'RODEO'
    elif 'ENTERPRISE RENT' in description or 'AMTRAK' in description:
        return 'OTHER_TRANSPORTATION'
    else:
        return 'OTHER'
    
def _categorize_all_credit_cards_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Apply transactions categorization to all transactions"""
    return (
        df
        .assign(category=df['description'].apply(_categorize_individual_credit_card_transaction))
    )

def _rename_chase_category_col(df: pd.DataFrame) -> pd.DataFrame:
    """Rename Chase credit card transaction category to avoid confusion with custom categories"""
    return (
        df
        .rename(columns={'category': 'chase_category'})
    )

def _update_credit_card_transactions_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Update categories after running initial categorization"""
    return (
        df
        .assign(category=lambda df_: np.select(
            condlist=[
                (df_['category'] == 'OVATION') & (df_['day_of_week'].isin([6, 7])),
                (df_['category'] == 'OVATION') & (~df_['day_of_week'].isin([6, 7])),
                (df_['category'] == 'OTHER') & (df_['chase_category'] == 'Food & Drink')
            ],
            choicelist=[
                'OVATION_WEEKEND',
                'OVATION_WEEKDAY',
                'EATING_OUT'
            ],
            default=df_['category']
            )
        )
    )

# Run all transformation functions on raw data for both data types
def clean_credit_card_data() -> pd.DataFrame:
    """Clean raw credit card transactions to to create staging table"""
    raw_cc_tx_tbl_query = """SELECT * FROM raw_credit_card_transactions"""
    df = pd.read_sql(raw_cc_tx_tbl_query, get_db().connection())

    # Apply transformations
    df = (
        df
        .pipe(_normalize_description)
        .pipe(_normalize_date)
        .pipe(_rename_chase_category_col)
        .pipe(_categorize_all_credit_cards_transactions)
        .pipe(_update_credit_card_transactions_categories)
    )
    
    return df


def clean_bank_account_data() -> pd.DataFrame:
    """Clean raw bank account transactions to to create staging table"""
    raw_bank_acc_tx_tbl_query = """SELECT * FROM raw_bank_account_transactions"""
    df = pd.read_sql(raw_bank_acc_tx_tbl_query, get_db().connection())

    # Apply transformations
    df = (
        df
        .pipe(_normalize_description)
        .pipe(_normalize_date)
        .pipe(_categorize_all_bank_transactions)
    )
    
    return df

# Create staging tables
def create_staging_bank_account_transactions() -> None:
    """Create staging table from cleaned data for bank account transactions"""
    df = clean_bank_account_data()
    
    # Write to staging table
    db = get_db()

    try:
        df.to_sql(
            'staging_bank_account_transactions', 
            db.connection(), 
            if_exists='replace', 
            index=False
        )
        db.commit()
        print(f"✅ Created staging_bank_account_transactions with {len(df)} rows")
    finally:
        db.close()
    

def create_staging_credit_card_transactions() -> None:
    """Create staging table from cleaned data for credit card transactions"""
    df = clean_credit_card_data()
    
    # Write to staging table
    db = get_db()

    try:
        df.to_sql(
            'staging_credit_card_transactions', 
            db.connection(), 
            if_exists='replace', 
            index=False
        )
        db.commit()
        print(f"✅ Created staging_credit_card_transactions with {len(df)} rows")
    finally:
        db.close()

