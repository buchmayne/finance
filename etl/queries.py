create_credit_card_tx_clean = """
-- Identify credit card payments
-- Create custom tags to better track spending patterns
CREATE TABLE credit_card_tx_clean AS
WITH tmp AS (
SELECT
    *
    , CAST(strftime('%Y', date) AS INTEGER) as year
    , CAST(strftime('%m', date) AS INTEGER) as month
    , CASE WHEN 
        description IN (
            'Payment Thank You-Mobile',
            'AUTOMATIC PAYMENT - THANK',
            'Payment Thank You - Web'
        ) AND amount >= 0 
        THEN true 
        ELSE false
        END AS credit_card_payment
    , CASE WHEN
        description = 'SQ *OVATION COFFEE &amp; TEA' THEN 'Ovation'
        WHEN description IN (
            'SQ *COFFEE TIME',
            'CAFFE UMBRIA PORTLAND',
            'SQ *SISTERS COFFEE COMPAN',
            'TST*CAFFE UMBRIA PORTLAN'
        ) THEN 'Other Coffee Shops'
        WHEN description IN (
            'CHIPOTLE ONLINE',
            'TST*PIZZICATO - PEARL',
            'TST* PIZZICATO - PEARL',
            'SQ *LOVEJOY BAKERS',
            'CHIPOTLE MEX GR ONLINE',
            'CHIPOTLE 1358',
            'TST* Pizzicato - Pearl'
        ) THEN 'Eating Out - Lunch'
        WHEN category = 'Gas' THEN 'Gas'
        WHEN description = 'DOMINO''S 9391' THEN 'Dominos'
        WHEN description = 'LUCKY LABRADOR BEER HALL' AND amount <= -150 THEN 'Lucky Lab Event Space - Trivia'
        WHEN description IN (
            'HAWTHORNE THEATER',
            'TST*REVOLUTION HALL',
            'TST* MCMENAMINS - CRYSTAL',
            'CASCADES AMPHITHEATRE'
        ) THEN 'Spending at Concerts/Events'
        WHEN description IN (
            'PAYMASTER LOUNGE',
            'THE FIELDS BAR &amp; GRILL',
            'TST* JERRY''S TAVERN',
            'JOES CELLAR',
            'SPO*THEFIELDSBAR&amp;GRILL'
        ) THEN 'Neighborhood Bars (Paymaster/Jerry''s/Fields/Joe''s)'
        WHEN description LIKE 'UBER%' OR description LIKE 'LYFT%' THEN 'Rideshare (Uber/Lyft)'
        WHEN description IN (
            'SAFEWAY #2790',
            'NEW SEASONS MARKET',
            'WHOLEFDS PRT 10148',
            'FRED-MEYER #0360',
            'ZUPAN''S MARKET',
            'COSTCO WHSE #0111',
            'ALBERTSONS #3531',
            'WHOLEFDS BRD 10266',
            'Whole Foods PRT 10148',
            'UWAJIMAYA',
            'TRADER JOE S #146',
            'THE MEATING PLACE',
            'WORLD FOODS',
            'COSTCO WHSE #0780',
            'CVS/PHARMACY #11282'
        ) THEN 'Groceries'
        WHEN description IN ('PORTLAND PARKING KITTY') THEN 'Parking (Street)'
        WHEN description IN ('OR LIQUOR STORE 245', 'OREGON LIQUOR STORE 164', 'OR LIQUOR STORE 185', 'SQ *ROLLING RIVER SPIRITS') THEN 'Liquor Store'
        WHEN description IN ('PORTLAND GENERAL ELECTRIC') THEN 'Utilities - PGE'
        WHEN description IN ('LA FITNESS', 'LA Fitness  *AnnualFee', 'LA FIT *ANNUALFEE') THEN 'Gym'
        WHEN description LIKE 'Spotify%' OR description LIKE 'SPOTIFY%' THEN 'Spotify'
        WHEN description IN ('COMCAST CABLE COMM', 'COMCAST / XFINITY') THEN 'Utilities - Comcast'
        WHEN description IN ('SQ *MICHELLE THRASHER', 'SQ *SLABTOWN BARBERSHOP') THEN 'Haircuts'
        WHEN description IN ('POWELL''S BURNSIDE') THEN 'Powells'
        WHEN description IN ('CTY CTR PARKING AUTO') THEN 'Parking (Monthly Off Street)'
        WHEN description IN (
            'REGAL CINEMAS INC',
            'CINEMA 21',
            'FOX TOWER STM 10',
            'HOLLYWOOD THEATRE',
            'LIVING ROOM THEATERS',
            'REGAL BRIDGEPORT  0652'
        ) THEN 'Movies'
        WHEN description IN (
            'BLACK BUTTE RANCH (1)',
            'ZOLA.COM*REGISTRY',
            'BLACK BUTTE RANCH FOOD',
            'IN *THE BOB LLC',
            'FORYOURPARTY',
            'SISTERS SALOON &amp; RANCH'
        ) THEN 'Wedding'
        WHEN description IN ('2MODA CENTER') THEN 'Moda Center'
        WHEN description IN ('SQ *PORTLAND INDOOR SOCCE', 'PORTLAND INDOOR SOCCER') THEN 'Indoor Soccer (Bar/Snacks/Membership)'
        WHEN description IN ('GEICO  *AUTO') THEN 'Car Insurance'
        WHEN description IN ('CLAUDE.AI SUBSCRIPTION', 'CHATGPT SUBSCRIPTION', 'OPENAI') THEN 'AI Subscriptions'
        WHEN description IN (
            'SQ *SHAKE SHACK',
            'MCDONALD''S F3972',
            'MCDONALD''S F6740',
            'BURGERVILLE - 14 - CONVEN',
            'MCDONALD''S F384',
            'MCDONALD''S F13589',
            'PAR*BURGERVILLE 14',
            'JACK IN THE BOX 7160'
        ) THEN 'Fast Food'
        WHEN description IN ('DIGITALOCEAN.COM', 'Amazon web services') THEN 'Trivia Website'
        WHEN description IN ('PDX AIRPORT PARKING') THEN 'Airport Parking'
        WHEN description IN (
            'SQ *GASTRO MANIA',
            'JOJO PEARL',
            'TST* WILD CHILD PIZZA - F',
            'MOMO YAMA',
            'TST* SIZZLE PIE - WEST',
            'TST* MISSISSIPPI STUDIOS',
            'TST* 10 BARREL BREWING -',
            'TST* SCOTTIE''S PIZZA PARL',
            'TST* BREAKSIDE BREWERY -',
            'TST* 10 BARREL PORTLAND N',
            'TST* QDS',
            'CARLITAS',
            'TST* SILVER HARBOR BREWIN',
            'TST*RIVER PIG - PORTLAND',
            'YAMA SUSHI AND SAKE BAR',
            'BANNINGS RESTAURANT &amp; PIE',
            'TST* GARDEN TAVERN',
            'TST* FIRE ON THE MOUNTAIN',
            'THE TRIPLE LINDY',
            'SQ *SCOTTIE''S PIZZA PARLO',
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
            'KINGPINS - BEAVERTON - BO'
        ) THEN 'Eating/Drinking Out - Other'
        WHEN description IN (
            'NORDSTROM DIRECT #0808',
            'PROPER CLOTH',
            'NORDSTROM #0020',
            'MORJAS',
            'Fjaellraeven Portland Ev',
            'GlobalE /Arsenal',
            'EUROPEAN MASTER TAILOR',
            'ON INC',
            'TOMMY BAHAMA 613',
            'WARBY PARKER',
            'Bonobos',
            'VINTAGE SPORTS FASHION',
            'GLOBAL-E* ARSENAL',
            'SP WADE AND WILLIAMS',
            'SP ANDAFTERTHAT',
            'NORDSTROM #0025'
        ) THEN 'Clothes'
        WHEN description IN (
            'EVERYDAY MUSIC',
            'CRITERION.COM',
            'BARNES&amp;NOBLE PAPERSOURCE',
            'BARNES &amp; NOBLE 2371',
            'Arrow Films',
            'MUSIC MILLENNIUM'
        ) THEN 'Blurays/Records'
        WHEN description IN ('USPS PO 4067900008') THEN 'Post Office'
        WHEN description IN ('Microsoft*Xbox', 'Microsoft*Xbox Live Gold', 'Microsoft*Xbox Game Pass', 'MICROSOFT*XBOX') THEN 'Xbox'
        WHEN description IN ('APPLE.COM/BILL') THEN 'Apple Cloud Storage'
        WHEN description IN ('Roku for WarnerMedia Glob') THEN 'HBO Subscription'
        WHEN description IN ('KITCHEN KABOODLE') THEN 'Kitchen Kaboodle'
        WHEN description IN ('GORGE PERFORMANCE', 'SP TRAVELERSURFCLUB') THEN 'Surfing'
        WHEN description IN (
            'SEATGEEK TICKETS',
            'AXS.COMFESTIVAL GV R',
            'TM *KAYTRANADA X JUSTI',
            'MCMENAMINS CONCERTS',
            'CASCADE TICKETS',
            'REVOLUTION HALL',
            'TCKTWEB*GOATWHOREVITRI',
            'PP*GATES TO HELL',
            'SQ *VITRIOL'
        ) THEN 'Concert/Event Tickets'
        WHEN description IN (
            'ALASKA AIR  0272398580252',
            'UNITED      0162417198919',
            'UNITED      0162319914519',
            'UNITED      0162319914520',
            'ALASKA AIR  0272113235962',
            'ALASKA AIR  0272398579726',
            'AMERICAN AIR0012481104340',
            'ALASKA AIR  0272361256749',
            'ALASKA AIR  0272398579634'
        ) THEN 'Flights'
        WHEN description IN (
            'WARWICK ALLERTON HOTEL',
            'HOOD RIVER HOTEL',
            'AIRBNB * HMPSDMXX99',
            'COURTYARD BY MARRIOTT',
            'MARRIOTT SN FRAN MARQU',
            'BEST WESTERN PONDEROSA'
        ) THEN 'Hotels/Airbnb'
        WHEN description IN ('SQ *301 GALLERY LLC') THEN 'Paintings'
        WHEN description IN ('DUNCD ON PRIME') THEN 'Podcast Subscription'
        WHEN description IN ('LES SCHWAB TIRES #0243', 'ODOT DMV2U', 'DEQ VIP DEQ TOO') THEN 'Car Maintenance'
        WHEN description IN ('BlazerVision') THEN 'Blazer Vision Subscription'
        WHEN description IN ('JEWELERS-MUTUAL-PMNT') THEN 'Diamond Insurance'
        WHEN description IN ('HRB ONLINE TAX PRODUCT') THEN 'Tax Software/Services'
        WHEN description LIKE 'AMAZON%' OR description LIKE 'Amazon%' OR description LIKE 'AMZN%' THEN 'Amazon' 
        WHEN description LIKE 'Prime Video%' THEN 'VOD Amazon'
        WHEN description IN ('WILLAMETTE DRY CLEAN &amp; A', 'WILLAMETTE DRYCLEAN AND A', 'Willamette Dry Clean &amp; Al') THEN 'Dry Cleaning'
        WHEN description IN ('SQ *ST. PAUL RODEO', 'SQ *ST. PAUL JAYCEES') THEN 'Rodeos'
        WHEN description IN (
            'Pendleton',
            'LULULEMON BRIDGEPORT',
            'HONEYFUND.COMGIFTCARDS',
            'SP KIRIKO',
            'SP BABYLIST',
            'SP WWW.POSHBABY.COM',
            'SQ *VIK ROASTERS',
            'SP ECRU MODERN STATI',
            'LS OBLATIONPAPERS.COM'
        ) THEN 'Gifts'
        WHEN description = 'PORTLAND ARTS TAX' THEN 'PORTLAND ARTS TAX'
        WHEN description = 'Chess.com Chess.com' THEN 'Chess.com Subscription'
        WHEN description IN ('DNH*DOMAINS#3405924658', 'GOOGLE *Domains') THEN 'Domain Name Subscription'
        WHEN description IN ('TCGPLAYER.COM', 'MAKEPLAYINGCARDS.COM') THEN 'MtG'
        WHEN description IN ('AMTRAK .COM 0420676093065', 'ENTERPRISE RENT-A-CAR') THEN 'Other Travel (Trains/Rental Cars)'
        WHEN description = 'PlayStation Network' THEN 'PlayStation Games'
        WHEN description IN ('Roku for Peacock TV LLC') THEN 'Peacock Subscription'
        WHEN description IN ('GOOGLE *Paramount', 'GOOGLE *CBS Mobile App') THEN 'Paramount Subscription'
        WHEN description IN (
            'PEARL HARDWARE',
            'CRATE &amp; BARREL #454',
            'Restoration Hardware',
            'THE HOME DEPOT 4002',
            'WILLIAMS-SONOMA 6324'
        ) THEN 'Home Improvement'
        WHEN description IN ('SP OPAL CAMERA') THEN 'Computers/Technology'
        -- Placeholders until full categorization
        WHEN category = 'Food & Drink' THEN 'Eating/Drinking Out - Other'
        WHEN category = 'Groceries' THEN 'Other Groceries'
        ELSE 'Other'
        END AS tag
FROM credit_card_transactions
)

SELECT * FROM tmp
"""

create_bank_account_tx_clean = """
CREATE TABLE bank_account_tx_clean AS
WITH tmp AS (
SELECT
    *
    , UPPER(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(description, '  ', ' '), 
                            '  ', ' '), 
                        '  ', ' '), 
                    '  ', ' '), 
                '  ', ' '), 
            '  ', ' ')
    ) AS clean_description
    , CAST(strftime('%Y', date) AS INTEGER) as year
    , CAST(strftime('%m', date) AS INTEGER) as month
FROM bank_account_transactions
),

tmp2 AS (
SELECT
    *
    , CASE WHEN clean_description LIKE 'CLEARCOVER INC PAYROLL%' 
        OR clean_description LIKE 'FEDEX DATAWORKS DIR DEP PPD ID%'
        OR clean_description LIKE 'ECONOMIC CONSULT PAYROLL PPD ID%'
        OR clean_description LIKE 'EMPLOYMT BENEFIT UI BENEFIT PPD ID%'
    THEN 'Salary'
    WHEN clean_description = 'INTEREST PAYMENT' THEN 'Interest Payment'
    WHEN clean_description LIKE 'VERIZON WIRELESS PAYMENTS PPD ID%' THEN 'Cell Phone Bill'
    WHEN clean_description LIKE 'VANGUARD BUY INVESTMENT%' THEN 'Transfer to Savings'
    WHEN clean_description LIKE 'VANGUARD SELL INVESTMENT%' 
        OR clean_description LIKE 'APA TREAS 310 MISC PAY PPD ID:%' THEN 'Transfer from Savings'
    WHEN clean_description LIKE 'VENMO PAYMENT%' THEN 'Venmo Payment'
    WHEN clean_description LIKE 'VENMO CASHOUT%' THEN 'Venmo Cashout'
    WHEN clean_description LIKE 'PINNACLE COA PINNACLE C%' THEN 'HOA Payment'
    WHEN clean_description LIKE 'CHASE CREDIT CRD AUTOPAY PPD ID%'
        OR clean_description LIKE 'PAYMENT TO CHASE CARD ENDING IN%'
    THEN 'Card Payment'
    WHEN clean_description LIKE 'ONPOINT COMMUNIT RE PAYMENT%' 
        OR clean_description LIKE 'ONPOINT COMM CU MTG PYMTS PPD ID%'
    THEN 'Mortgage Payment'
    WHEN clean_description LIKE 'ONLINE TRANSFER TO SAV%' 
        OR clean_description LIKE 'ONLINE TRANSFER TO CHK%'    
        OR clean_description LIKE 'ONLINE TRANSFER FROM CHK%'   
        OR clean_description LIKE 'ONLINE TRANSFER FROM SAV%'   
    THEN 'Transfer Between Accounts'
    WHEN clean_description LIKE 'DEPOSIT ID NUMBER%' OR clean_description LIKE 'REMOTE ONLINE DEPOSIT%' THEN 'Cash Deposit'
    WHEN clean_description LIKE 'WITHDRAWAL%' THEN 'Cash Withdrawl'
    WHEN clean_description LIKE 'ALEX ELISE PHOTO ALEX ELISE ST%' THEN 'Wedding Photographer'
    WHEN clean_description LIKE 'WEX HEALTH PREMIUMS 28670940 WEB ID%' THEN 'Health Insurance COBRA'
    WHEN clean_description LIKE 'OR REVENUE DEPT ORSTTAXRFD PPD ID%'
        OR clean_description LIKE 'IRS TREAS 310 TAX REF PPD ID%'
    THEN 'Tax Refund'
    WHEN clean_description LIKE 'CHECK # 1976 PASSPORTSERVICES PAYMENT ARC ID%' THEN 'Passport Renewal Payment'
    WHEN clean_description LIKE 'WESTFIELD BANK ACCTVERIFY PPD I%'
        OR clean_description LIKE 'WESTFIELD BANK ACCTVERIFY 10360909219%'
        OR clean_description LIKE 'WESTBK CK WEBXFR P2P JENNA CARLSON WEB%'
    THEN 'Jenna Account Transfer for Wedding Payments'
    ELSE 'Other'
    END AS tag
FROM tmp
)

SELECT
    *
FROM tmp2

"""