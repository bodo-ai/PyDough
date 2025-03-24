-- Modified version of the defog.ai schema setup for the broker schema:
-- https://github.com/defog-ai/defog-data/blob/main/defog_data/broker/broker.sql
-- changed slightly to use SQLite dialect instead of postgress.

-------------------------------------------------------------------------------
--BROKER SCHEMA 

-- Dimension tables
CREATE TABLE sbCustomer (
  sbCustId varchar(20) PRIMARY KEY,
  sbCustName varchar(100) NOT NULL,
  sbCustEmail varchar(100) NOT NULL,
  sbCustPhone varchar(20),
  sbCustAddress1 varchar(200),
  sbCustAddress2 varchar(200),
  sbCustCity varchar(50),
  sbCustState varchar(20),
  sbCustCountry varchar(50),
  sbCustPostalCode varchar(20),
  sbCustJoinDate date NOT NULL,
  sbCustStatus varchar(20) NOT NULL -- possible values: active, inactive, suspended, closed
);

CREATE TABLE sbTicker (
  sbTickerId varchar(20) PRIMARY KEY,
  sbTickerSymbol varchar(10) NOT NULL,
  sbTickerName varchar(100) NOT NULL, 
  sbTickerType varchar(20) NOT NULL, -- possible values: stock, etf, mutualfund
  sbTickerExchange varchar(50) NOT NULL,
  sbTickerCurrency varchar(10) NOT NULL,
  sbTickerDb2x varchar(20), -- 2 letter exchange code
  sbTickerIsActive boolean NOT NULL
);

-- Fact tables  
CREATE TABLE sbDailyPrice (
  sbDpTickerId varchar(20) NOT NULL,
  sbDpDate date NOT NULL,
  sbDpOpen numeric(10,2) NOT NULL,
  sbDpHigh numeric(10,2) NOT NULL, 
  sbDpLow numeric(10,2) NOT NULL,
  sbDpClose numeric(10,2) NOT NULL,
  sbDpVolume bigint NOT NULL,
  sbDpEpochMs bigint NOT NULL, -- epoch milliseconds for timestamp
  sbDpSource varchar(50)
);

CREATE TABLE sbTransaction (
  sbTxId varchar(50) PRIMARY KEY,
  sbTxCustId varchar(20) NOT NULL,
  sbTxTickerId varchar(20) NOT NULL,
  sbTxDateTime timestamp NOT NULL,
  sbTxType varchar(20) NOT NULL, -- possible values: buy, sell
  sbTxShares numeric(10,2) NOT NULL,
  sbTxPrice numeric(10,2) NOT NULL,
  sbTxAmount numeric(10,2) NOT NULL,
  sbTxCcy varchar(10), -- transaction currency  
  sbTxTax numeric(10,2) NOT NULL,
  sbTxCommission numeric(10,2) NOT NULL,
  sbTxKpx varchar(10), -- internal code
  sbTxSettlementDateStr varchar(25), -- settlement date as string in yyyyMMdd HH:mm:ss format. NULL if not settled
  sbTxStatus varchar(10) NOT NULL -- possible values: success, fail, pending
);


-- sbCustomer
INSERT INTO sbCustomer (sbCustId, sbCustName, sbCustEmail, sbCustPhone, sbCustAddress1, sbCustCity, sbCustState, sbCustCountry, sbCustPostalCode, sbCustJoinDate, sbCustStatus) VALUES
('C001', 'john doe', 'john.doe@email.com', '555-123-4567', '123 Main St', 'Anytown', 'CA', 'USA', '90001', '2020-01-01', 'active'),
('C002', 'Jane Smith', 'jane.smith@email.com', '555-987-6543', '456 Oak Rd', 'Someville', 'NY', 'USA', '10002', '2019-03-15', 'active'),
('C003', 'Bob Johnson', 'bob.johnson@email.com', '555-246-8135', '789 Pine Ave', 'Mytown', 'TX', 'USA', '75000', '2022-06-01', 'inactive'),
('C004', 'Samantha Lee', 'samantha.lee@email.com', '555-135-7902', '246 Elm St', 'Yourtown', 'CA', 'USA', '92101', '2018-09-22', 'suspended'),
('C005', 'Michael Chen', 'michael.chen@email.com', '555-864-2319', '159 Cedar Ln', 'Anothertown', 'FL', 'USA', '33101', '2021-02-28', 'active'),
('C006', 'Emily Davis', 'emily.davis@email.com', '555-753-1904', '753 Maple Dr', 'Mytown', 'TX', 'USA', '75000', '2020-07-15', 'active'), 
('C007', 'David Kim', 'david.kim@email.com', '555-370-2648', '864 Oak St', 'Anothertown', 'FL', 'USA', '33101', '2022-11-05', 'active'),
('C008', 'Sarah Nguyen', 'sarah.nguyen@email.com', '555-623-7419', '951 Pine Rd', 'Yourtown', 'CA', 'USA', '92101', '2019-04-01', 'closed'),
('C009', 'William Garcia', 'william.garcia@email.com', '555-148-5326', '258 Elm Ave', 'Anytown', 'CA', 'USA', '90001', '2021-08-22', 'active'),
('C010', 'Jessica Hernandez', 'jessica.hernandez@email.com', '555-963-8520', '147 Cedar Blvd', 'Someville', 'NY', 'USA', '10002', '2020-03-10', 'inactive'),
('C011', 'Alex Rodriguez', 'alex.rodriguez@email.com', '555-246-1357', '753 Oak St', 'Newtown', 'NJ', 'USA', '08801', '2023-01-15', 'active'),
('C012', 'Olivia Johnson', 'olivia.johnson@email.com', '555-987-6543', '321 Elm St', 'Newtown', 'NJ', 'USA', '08801', '2023-01-05', 'active'),
('C013', 'Ethan Davis', 'ethan.davis@email.com', '555-246-8135', '654 Oak Ave', 'Someville', 'NY', 'USA', '10002', '2023-02-12', 'active'),
('C014', 'Ava Wilson', 'ava.wilson@email.com', '555-135-7902', '987 Pine Rd', 'Anytown', 'CA', 'USA', '90001', '2023-03-20', 'active'),
('C015', 'Emma Brown', 'emma.brown@email.com', '555-987-6543', '789 Oak St', 'Newtown', 'NJ', 'USA', '08801', DATE('NOW', 'start of month', '-5 months'), 'active'),
('C016', 'sophia martinez', 'sophia.martinez@email.com', '555-246-8135', '159 Elm Ave', 'Anytown', 'CA', 'USA', '90001', DATE('NOW', 'start of month', '-4 months'), 'active'),
('C017', 'Jacob Taylor', 'jacob.taylor@email.com', '555-135-7902', '753 Pine Rd', 'Someville', 'NY', 'USA', '10002', DATE('NOW', 'start of month', '-3 months'), 'active'),
('C018', 'Michael Anderson', 'michael.anderson@email.com', '555-864-2319', '321 Cedar Ln', 'Yourtown', 'CA', 'USA', '92101', DATE('NOW', 'start of month', '-2 months'), 'active'),
('C019', 'Isabella Thompson', 'isabella.thompson@email.com', '555-753-1904', '987 Maple Dr', 'Anothertown', 'FL', 'USA', '33101', DATE('NOW', 'start of month', '-1 months'), 'active'),
('C020', 'Maurice Lee', 'maurice.lee@email.com', '555-370-2648', '654 Oak St', 'Mytown', 'TX', 'USA', '75000', DATE('NOW', 'start of month'), 'active');


-- sbTicker  
INSERT INTO sbTicker (sbTickerId, sbTickerSymbol, sbTickerName, sbTickerType, sbTickerExchange, sbTickerCurrency, sbTickerDb2x, sbTickerIsActive) VALUES
('T001', 'AAPL', 'Apple Inc.', 'stock', 'NASDAQ', 'USD', 'NQ', true),
('T002', 'MSFT', 'Microsoft Corporation', 'stock', 'NASDAQ', 'USD', 'NQ', true),
('T003', 'AMZN', 'Amazon.com, Inc.', 'stock', 'NASDAQ', 'USD', 'NQ', true),
('T004', 'TSLA', 'Tesla, Inc.', 'stock', 'NASDAQ', 'USD', 'NQ', true),
('T005', 'GOOGL', 'Alphabet Inc.', 'stock', 'NASDAQ', 'USD', 'NQ', true),
('T006', 'FB', 'Meta Platforms, Inc.', 'stock', 'NASDAQ', 'USD', 'NQ', true),
('T007', 'BRK.B', 'Berkshire Hathaway Inc.', 'stock', 'NYSE', 'USD', 'NY', true),
('T008', 'JPM', 'JPMorgan Chase & Co.', 'stock', 'NYSE', 'USD', 'NY', true),
('T009', 'V', 'Visa Inc.', 'stock', 'NYSE', 'USD', 'NY', true),
('T010', 'PG', 'Procter & Gamble Company', 'stock', 'NYSE', 'USD', 'NY', true),
('T011', 'SPY', 'SPDR S&P 500 ETF Trust', 'etf', 'NYSE Arca', 'USD', 'NX', true),
('T012', 'QQQ', 'Invesco QQQ Trust', 'etf', 'NASDAQ', 'USD', 'NQ', true),
('T013', 'VTI', 'Vanguard Total Stock Market ETF', 'etf', 'NYSE Arca', 'USD', 'NX', true), 
('T014', 'VXUS', 'Vanguard Total International Stock ETF', 'etf', 'NASDAQ', 'USD', 'NQ', true),
('T015', 'VFINX', 'Vanguard 500 Index Fund', 'mutualfund', 'Vanguard', 'USD', 'VG', true),
('T016', 'VTSAX', 'Vanguard Total Stock Market Index Fund', 'mutualfund', 'Vanguard', 'USD', 'VG', true),  
('T017', 'VIGAX', 'Vanguard Growth Index Fund', 'mutualfund', 'Vanguard', 'USD', 'VG', true),
('T018', 'GOOG', 'Alphabet Inc.', 'stock', 'NASDAQ', 'USD', 'NQ', true),
('T019', 'VTI', 'Vanguard Total Stock Market ETF', 'etf', 'NYSE Arca', 'USD', 'NX', true),
('T020', 'VTSAX', 'Vanguard Total Stock Market Index Fund', 'mutualfund', 'Vanguard', 'USD', 'VG', true),
('T021', 'NFLX', 'Netflix, Inc.', 'stock', 'NASDAQ', 'USD', 'NQ', true);

-- sbDailyPrice
INSERT INTO sbDailyPrice (sbDpTickerId, sbDpDate, sbDpOpen, sbDpHigh, sbDpLow, sbDpClose, sbDpVolume, sbDpEpochMs, sbDpSource) VALUES
('T001', '2023-04-01', 150.00, 152.50, 148.75, 151.25, 75000000, 1680336000000, 'NYSE'),
('T002', '2023-04-01', 280.00, 282.75, 279.50, 281.00, 35000000, 1680336000000, 'NASDAQ'),
('T003', '2023-04-01', 3200.00, 3225.00, 3180.00, 3210.00, 4000000, 1680336000000, 'NASDAQ'),
('T004', '2023-04-01', 180.00, 185.00, 178.50, 184.25, 20000000, 1680336000000, 'NASDAQ'),
('T005', '2023-04-01', 2500.00, 2525.00, 2475.00, 2510.00, 1500000, 1680336000000, 'NASDAQ'),
('T006', '2023-04-01', 200.00, 205.00, 198.00, 202.50, 15000000, 1680336000000, 'NASDAQ'),
('T007', '2023-04-01', 400000.00, 402500.00, 398000.00, 401000.00, 10000, 1680336000000, 'NYSE'),
('T008', '2023-04-01', 130.00, 132.50, 128.75, 131.00, 12000000, 1680336000000, 'NYSE'),
('T009', '2023-04-01', 220.00, 222.50, 218.00, 221.00, 8000000, 1680336000000, 'NYSE'),
('T010', '2023-04-01', 140.00, 142.00, 139.00, 141.50, 6000000, 1680336000000, 'NYSE'),
('T001', '2023-04-02', 151.50, 153.00, 150.00, 152.00, 70000000, 1680422400000, 'NYSE'),
('T002', '2023-04-02', 281.25, 283.50, 280.00, 282.75, 32000000, 1680422400000, 'NASDAQ'),
('T003', '2023-04-02', 3212.00, 3230.00, 3200.00, 3225.00, 3800000, 1680422400000, 'NASDAQ'),
('T004', '2023-04-02', 184.50, 187.00, 183.00, 186.00, 18000000, 1680422400000, 'NASDAQ'),
('T005', '2023-04-02', 2512.00, 2530.00, 2500.00, 2520.00, 1400000, 1680422400000, 'NASDAQ'),
('T006', '2023-04-02', 203.00, 206.50, 201.00, 205.00, 14000000, 1680422400000, 'NASDAQ'),
('T007', '2023-04-02', 401500.00, 403000.00, 400000.00, 402000.00, 9500, 1680422400000, 'NYSE'),
('T008', '2023-04-02', 131.25, 133.00, 130.00, 132.50, 11000000, 1680422400000, 'NYSE'),
('T009', '2023-04-02', 221.50, 223.00, 220.00, 222.00, 7500000, 1680422400000, 'NYSE'),
('T010', '2023-04-02', 141.75, 143.00, 140.50, 142.25, 5500000, 1680422400000, 'NYSE'),
('T001', '2023-04-03', 152.25, 154.00, 151.00, 153.50, 65000000, 1680508800000, 'NYSE'),
('T002', '2023-04-03', 283.00, 285.00, 281.50, 284.00, 30000000, 1680508800000, 'NASDAQ'),
('T003', '2023-04-03', 3227.00, 3240.00, 3220.00, 3235.00, 3600000, 1680508800000, 'NASDAQ'),
('T004', '2023-04-03', 186.25, 188.50, 185.00, 187.75, 16000000, 1680508800000, 'NASDAQ'),
('T005', '2023-04-03', 2522.00, 2540.00, 2515.00, 2535.00, 1300000, 1680508800000, 'NASDAQ'),  
('T006', '2023-04-03', 205.50, 208.00, 203.50, 207.00, 13000000, 1680508800000, 'NASDAQ'),
('T007', '2023-04-03', 402500.00, 404000.00, 401000.00, 403500.00, 9000, 1680508800000, 'NYSE'),
('T008', '2023-04-03', 132.75, 134.50, 131.50, 133.75, 10000000, 1680508800000, 'NYSE'),
('T009', '2023-04-03', 222.25, 224.00, 221.00, 223.50, 7000000, 1680508800000, 'NYSE'),
('T010', '2023-04-03', 142.50, 144.00, 141.50, 143.25, 5000000, 1680508800000, 'NYSE'),
('T019', DATE('now', '-8 days'), 204.00, 204.50, 202.75, 203.25, 8000000, STRFTIME('%s', DATE('NOW', '-8 days')) * 1000, 'NYSE'),
('T019', DATE('now', '-6 days'), 205.00, 207.50, 203.75, 206.25, 8000000, STRFTIME('%s', DATE('NOW', '-6 days')) * 1000, 'NYSE'),
('T019', DATE('now', '-5 days'), 206.50, 208.00, 205.00, 207.00, 7500000, STRFTIME('%s', DATE('NOW', '-5 days')) * 1000, 'NYSE'),
('T019', DATE('now', '-4 days'), 207.25, 209.00, 206.50, 208.50, 7000000, STRFTIME('%s', DATE('NOW', '-4 days')) * 1000, 'NYSE'),
('T019', DATE('now', '-3 days'), 208.75, 210.50, 207.75, 209.75, 6500000, STRFTIME('%s', DATE('NOW', '-3 days')) * 1000, 'NYSE'),
('T019', DATE('now', '-2 days'), 210.00, 211.75, 209.25, 211.00, 6000000, STRFTIME('%s', DATE('NOW', '-2 days')) * 1000, 'NYSE'),
('T019', DATE('now', '-1 day'), 211.25, 213.00, 210.50, 212.25, 5500000, STRFTIME('%s', DATE('NOW', '-1 days')) * 1000, 'NYSE'),
('T019', DATE('now'), 212.50, 214.25, 211.75, 213.50, 5000000, STRFTIME('%s', DATE('NOW')) * 1000, 'NYSE'),
('T020', DATE('now', '-6 days'), 82.00, 83.00, 81.50, 82.50, 1000000, STRFTIME('%s', DATE('NOW', '-6 days')) * 1000, 'Vanguard'),  
('T020', DATE('now', '-5 days'), 82.60, 83.60, 82.10, 83.10, 950000, STRFTIME('%s', DATE('NOW', '-5 days')) * 1000, 'Vanguard'),
('T020', DATE('now', '-4 days'), 83.20, 84.20, 82.70, 83.70, 900000, STRFTIME('%s', DATE('NOW', '-4 days')) * 1000, 'Vanguard'),  
('T020', DATE('now', '-3 days'), 83.80, 84.80, 83.30, 84.30, 850000, STRFTIME('%s', DATE('NOW', '-3 days')) * 1000, 'Vanguard'),
('T020', DATE('now', '-2 days'), 84.40, 85.40, 83.90, 84.90, 800000, STRFTIME('%s', DATE('NOW', '-2 days')) * 1000, 'Vanguard'),
('T020', DATE('now', '-1 day'), 85.00, 86.00, 84.50, 85.50, 750000, STRFTIME('%s', DATE('NOW', '-1 days')) * 1000, 'Vanguard'),  
('T020', DATE('now'), 85.60, 86.60, 85.10, 86.10, 700000, STRFTIME('%s', DATE('NOW')) * 1000, 'Vanguard'),
('T021', DATE('now', '-6 days'), 300.00, 305.00, 297.50, 302.50, 10000000, STRFTIME('%s', DATE('NOW', '-6 days')) * 1000, 'NASDAQ'),
('T021', DATE('now', '-5 days'), 303.00, 308.00, 300.50, 305.50, 9500000, STRFTIME('%s', DATE('NOW', '-5 days')) * 1000, 'NASDAQ'),  
('T021', DATE('now', '-4 days'), 306.00, 311.00, 303.50, 308.50, 9000000, STRFTIME('%s', DATE('NOW', '-4 days')) * 1000, 'NASDAQ'),
('T021', DATE('now', '-3 days'), 309.00, 314.00, 306.50, 311.50, 8500000, STRFTIME('%s', DATE('NOW', '-3 days')) * 1000, 'NASDAQ'),
('T021', DATE('now', '-2 days'), 312.00, 317.00, 309.50, 314.50, 8000000, STRFTIME('%s', DATE('NOW', '-2 days')) * 1000, 'NASDAQ'),  
('T021', DATE('now', '-1 day'), 315.00, 320.00, 312.50, 317.50, 7500000, STRFTIME('%s', DATE('NOW', '-1 days')) * 1000, 'NASDAQ'),
('T021', DATE('now'), 318.00, 323.00, 315.50, 320.50, 7000000, STRFTIME('%s', DATE('NOW')) * 1000, 'NASDAQ');

-- sbTransaction
INSERT INTO sbTransaction (sbTxId, sbTxCustId, sbTxTickerId, sbTxDateTime, sbTxType, sbTxShares, sbTxPrice, sbTxAmount, sbTxCcy, sbTxTax, sbTxCommission, sbTxKpx, sbTxSettlementDateStr, sbTxStatus) VALUES
('TX001', 'C001', 'T001', '2023-04-01 09:30:00', 'buy', 100, 150.00, 15000.00, 'USD', 75.00, 10.00, 'KP001', '20230401 09:30:00', 'success'),
('TX002', 'C002', 'T002', '2023-04-01 10:15:00', 'sell', 50, 280.00, 14000.00, 'USD', 70.00, 10.00, 'KP002', '20230401 10:15:00', 'success'),
('TX003', 'C003', 'T003', '2023-04-01 11:00:00', 'buy', 10, 3200.00, 32000.00, 'USD', 160.00, 20.00, 'KP003', '20230401 11:00:00', 'success'),
('TX004', 'C003', 'T004', '2023-04-01 11:45:00', 'sell', 25, 180.00, 4500.00, 'USD', 22.50, 5.00, 'KP004', '20230401 11:45:00', 'success'),
('TX005', 'C005', 'T005', '2023-04-01 12:30:00', 'buy', 5, 2500.00, 12500.00, 'USD', 62.50, 15.00, 'KP005', '20230401 12:30:00', 'success'),
('TX006', 'C002', 'T006', '2023-04-01 13:15:00', 'sell', 75, 200.00, 15000.00, 'USD', 75.00, 10.00, 'KP006', '20230401 13:15:00', 'success'),
('TX007', 'C003', 'T007', '2023-04-01 14:00:00', 'buy', 1, 400000.00, 400000.00, 'USD', 2000.00, 100.00, 'KP007', '20230401 14:00:00', 'success'),
('TX008', 'C003', 'T008', '2023-04-01 14:45:00', 'sell', 100, 130.00, 13000.00, 'USD', 65.00, 10.00, 'KP008', '20230401 14:45:00', 'success'),
('TX009', 'C009', 'T009', '2023-04-01 15:30:00', 'buy', 50, 220.00, 11000.00, 'USD', 55.00, 10.00, 'KP009', '20230401 15:30:00', 'success'),
('TX010', 'C002', 'T010', '2023-04-01 16:15:00', 'sell', 80, 140.00, 11200.00, 'USD', 56.00, 10.00, 'KP010', '20230401 16:15:00', 'success'),
('TX011', 'C001', 'T001', '2023-04-02 09:30:00', 'sell', 50, 151.50, 7575.00, 'USD', 37.88, 5.00, 'KP011', '20230402 09:30:00', 'success'),
('TX012', 'C002', 'T002', '2023-04-02 10:15:00', 'buy', 30, 281.25, 8437.50, 'USD', 42.19, 7.50, 'KP012', '20230402 10:15:00', 'fail'),
('TX013', 'C003', 'T003', '2023-04-02 11:00:00', 'sell', 5, 3212.00, 16060.00, 'USD', 80.30, 15.00, 'KP013', '20230402 11:00:00', 'success'), 
('TX014', 'C004', 'T004', '2023-04-02 11:45:00', 'buy', 15, 184.50, 2767.50, 'USD', 13.84, 5.00, 'KP014', '20230402 11:45:00', 'success'),
('TX015', 'C005', 'T005', '2023-04-02 12:30:00', 'sell', 2, 2512.00, 5024.00, 'USD', 25.12, 10.00, 'KP015', '20230402 12:30:00', 'success'),
('TX016', 'C006', 'T006', '2023-04-02 13:15:00', 'buy', 50, 203.00, 10150.00, 'USD', 50.75, 10.00, 'KP016', '20230402 13:15:00', 'success'),  
('TX017', 'C007', 'T007', '2023-04-02 14:00:00', 'sell', 1, 401500.00, 401500.00, 'USD', 2007.50, 100.00, 'KP017', '20230402 14:00:00', 'success'),
('TX018', 'C008', 'T008', '2023-04-02 14:45:00', 'buy', 75, 131.25, 9843.75, 'USD', 49.22, 7.50, 'KP018', '20230402 14:45:00', 'success'),
('TX019', 'C009', 'T009', '2023-04-02 15:30:00', 'sell', 25, 221.50, 5537.50, 'USD', 27.69, 5.00, 'KP019', '20230402 15:30:00', 'success'),
('TX020', 'C010', 'T010', '2023-04-02 16:15:00', 'buy', 60, 141.75, 8505.00, 'USD', 42.53, 7.50, 'KP020', '20230402 16:15:00', 'success'),
('TX021', 'C001', 'T001', '2023-04-03 09:30:00', 'buy', 75, 152.25, 11418.75, 'USD', 57.09, 10.00, 'KP021', '20230403 09:30:00', 'fail'),
('TX022', 'C002', 'T002', '2023-04-03 10:15:00', 'sell', 40, 283.00, 11320.00, 'USD', 56.60, 10.00, 'KP022', '20230403 10:15:00', 'success'),
('TX023', 'C003', 'T003', '2023-04-03 11:00:00', 'buy', 8, 3227.00, 25816.00, 'USD', 129.08, 20.00, 'KP023', '20230403 11:00:00', 'success'),
('TX024', 'C004', 'T004', '2023-04-03 11:45:00', 'sell', 20, 186.25, 3725.00, 'USD', 18.63, 5.00, 'KP024', '20230403 11:45:00', 'success'),
('TX025', 'C005', 'T005', '2023-04-03 12:30:00', 'buy', 3, 2522.00, 7566.00, 'USD', 37.83, 15.00, 'KP025', '20230403 12:30:00', 'success'),
('TX026', 'C006', 'T006', '2023-04-03 13:15:00', 'sell', 60, 205.50, 12330.00, 'USD', 61.65, 10.00, 'KP026', '20230403 13:15:00', 'success'),
('TX027', 'C007', 'T007', '2023-04-03 14:00:00', 'buy', 1, 402500.00, 402500.00, 'USD', 2012.50, 100.00, 'KP027', '20230403 14:00:00', 'success'),  
('TX028', 'C008', 'T008', '2023-04-03 14:45:00', 'sell', 90, 132.75, 11947.50, 'USD', 59.74, 7.50, 'KP028', '20230403 14:45:00', 'success'),
('TX029', 'C009', 'T009', '2023-04-03 15:30:00', 'buy', 40, 222.25, 8890.00, 'USD', 44.45, 10.00, 'KP029', '20230403 15:30:00', 'success'),
('TX030', 'C010', 'T010', '2023-04-03 16:15:00', 'sell', 70, 142.50, 9975.00, 'USD', 49.88, 10.00, 'KP030', '20230403 16:15:00', 'success'),
('TX031', 'C001', 'T001', DATE('now', '-9 days'), 'buy', 100, 150.00, 15000.00, 'USD', 75.00, 10.00, 'KP031', NULL, 'fail'),
('TX032', 'C002', 'T002', DATE('now', '-8 days'), 'sell', 80, 280.00, 14000.00, 'USD', 70.00, 10.00, 'KP032', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', '-8 days')), 'success'),
('TX033', 'C003', 'T001', DATE('now', '-7 days'), 'buy', 120, 200.00, 24000.00, 'USD', 120.00, 15.00, 'KP033', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', '-7 days')), 'success'),
('TX034', 'C004', 'T004', DATE('now', '-6 days'), 'sell', 90, 320.00, 28800.00, 'USD', 144.00, 12.00, 'KP034', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', '-6 days')), 'success'),
('TX035', 'C005', 'T001', DATE('now', '-5 days'), 'buy', 150, 180.00, 27000.00, 'USD', 135.00, 20.00, 'KP035', NULL, 'fail'),
('TX036', 'C006', 'T006', DATE('now', '-4 days'), 'sell', 70, 300.00, 21000.00, 'USD', 105.00, 15.00, 'KP036', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', '-4 days')), 'success'),
('TX037', 'C007', 'T007', DATE('now', '-3 days'), 'buy', 110, 220.00, 24200.00, 'USD', 121.00, 10.00, 'KP037', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', '-3 days')), 'success'),
('TX038', 'C008', 'T008', DATE('now', '-2 days'), 'sell', 100, 350.00, 35000.00, 'USD', 175.00, 25.00, 'KP038', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', '-2 days')), 'success'),
('TX039', 'C009', 'T007', DATE('now', '-1 day'), 'buy', 80, 230.00, 18400.00, 'USD', 92.00, 18.00, 'KP039', NULL, 'pending'),
('TX040', 'C001', 'T011', DATE('now', '-10 days'), 'buy', 50, 400.00, 20000.00, 'USD', 100.00, 20.00, 'KP040', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', '-10 days')), 'success'),
('TX041', 'C002', 'T012', DATE('now', '-9 days'), 'sell', 30, 320.00, 9600.00, 'USD', 48.00, 15.00, 'KP041', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', '-9 days')), 'success'),
('TX042', 'C003', 'T013', DATE('now', '-8 days'), 'buy', 80, 180.00, 14400.00, 'USD', 72.00, 10.00, 'KP042', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', '-8 days')), 'success'),
('TX043', 'C004', 'T014', DATE('now', '-7 days'), 'sell', 60, 220.00, 13200.00, 'USD', 66.00, 12.00, 'KP043', NULL, 'pending'),
('TX044', 'C012', 'T001', '2023-01-15 10:00:00', 'buy', 80, 155.00, 12400.00, 'USD', 62.00, 10.00, 'KP044', '20230115 10:00:00', 'success'),
('TX045', 'C012', 'T001', '2023-01-16 10:30:00', 'buy', 80, 155.00, 12400.00, 'USD', 62.00, 10.00, 'KP045', '20230116 10:30:00', 'success'),
('TX046', 'C013', 'T002', '2023-02-20 11:30:00', 'sell', 60, 285.00, 17100.00, 'USD', 85.50, 15.00, 'KP046', '20230220 11:30:00', 'success'),
('TX047', 'C014', 'T003', '2023-03-25 14:45:00', 'buy', 5, 3250.00, 16250.00, 'USD', 81.25, 20.00, 'KP047', '20230325 14:45:00', 'success'),
('TX048', 'C012', 'T004', '2023-01-30 13:15:00', 'sell', 40, 190.00, 7600.00, 'USD', 38.00, 10.00, 'KP048', '20230130 13:15:00', 'success'),
('TX049', 'C013', 'T005', '2023-02-28 16:00:00', 'buy', 2, 2550.00, 5100.00, 'USD', 25.50, 15.00, 'KP049', '20230228 16:00:00', 'success'),
('TX050', 'C014', 'T006', '2023-03-30 09:45:00', 'sell', 30, 210.00, 6300.00, 'USD', 31.50, 10.00, 'KP050', '20230331 09:45:00', 'success'),
('TX051', 'C015', 'T001', DATE('now', 'start of month', '5 months', '1 day'), 'buy', 50, 150.00, 7500.00, 'USD', 37.50, 10.00, 'KP051', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', 'start of month', '-5 months', '+1 day')), 'success'),
('TX052', 'C016', 'T002', DATE('now', 'start of month', '4 months', '2 days'), 'sell', 40, 280.00, 11200.00, 'USD', 56.00, 10.00, 'KP052', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', 'start of month', '-4 months', '+2 days')), 'success'),
('TX053', 'C017', 'T003', DATE('now', 'start of month', '3 months', '3 days'), 'buy', 15, 3200.00, 48000.00, 'USD', 240.00, 20.00, 'KP053', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', 'start of month', '-3 months', '+3 days')), 'success'),
('TX054', 'C018', 'T004', DATE('now', 'start of month', '2 months', '4 days'), 'sell', 30, 180.00, 5400.00, 'USD', 27.00, 5.00, 'KP054', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', 'start of month', '-2 months', '+4 days')), 'success'),
('TX055', 'C019', 'T005', DATE('now', 'start of month', '1 month', '5 days'), 'buy', 10, 2500.00, 25000.00, 'USD', 125.00, 15.00, 'KP055', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', 'start of month', '-1 months', '+5 days')), 'success'),
('TX056', 'C002', 'T006', DATE('now', 'start of month', '1 day'), 'sell', 20, 200.00, 4000.00, 'USD', 20.00, 10.00, 'KP056', STRFTIME('%Y%m%d %H:%i:%s', DATE('NOW', 'start of month', '+1 day')), 'success');


-------------------------------------------------------------------------------
--EWALLET SCHEMA 

-- Dimension tables
CREATE TABLE users (uid INTEGER, username TEXT(50) NOT NULL, email TEXT(100) NOT NULL, phone_number TEXT(20),
 created_at TIMESTAMP NOT NULL, last_login_at TIMESTAMP, user_type TEXT(20) NOT NULL /* possible values: individual, business, admin */, status TEXT(20) NOT NULL /* possible values: active, inactive, suspended, deleted */, country TEXT(2) /* 2-letter country code */, address_billing TEXT, address_delivery TEXT, kyc_status TEXT(20) /* possible values: pending, approved, rejected */, kyc_verified_at TIMESTAMP);

CREATE TABLE merchants (mid INTEGER, name TEXT(100) NOT NULL, description TEXT, website_url TEXT(200),
 logo_url TEXT(200),
 created_at TIMESTAMP NOT NULL, country TEXT(2) /* 2-letter country code */, state TEXT(50),
 city TEXT(50),
 postal_code TEXT(20),
 address TEXT, status TEXT(20) NOT NULL /* possible values: active, inactive, suspended */, category TEXT(50),
 sub_category TEXT(50),
 mcc INTEGER /* Merchant Category Code */, contact_name TEXT(100),
 contact_email TEXT(100),
 contact_phone TEXT(20));

CREATE TABLE coupons (cid INTEGER, merchant_id INTEGER NOT NULL , code TEXT(20) NOT NULL, description TEXT, start_date DATE NOT NULL, end_date DATE NOT NULL, discount_type TEXT(20) NOT NULL /* possible values: percentage, fixed_amount */, discount_value REAL(10, 2) NOT NULL, min_purchase_amount REAL(10, 2),
 max_discount_amount REAL(10, 2),
 redemption_limit INTEGER, status TEXT(20) NOT NULL /* possible values: active, inactive, expired */, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP);

CREATE TABLE wallet_transactions_daily (txid INTEGER PRIMARY KEY, sender_id INTEGER NOT NULL, sender_type INTEGER NOT NULL /* 0 for user, 1 for merchant */, receiver_id INTEGER NOT NULL, receiver_type INTEGER NOT NULL /* 0 for user, 1 for merchant */, amount REAL(10, 2) NOT NULL, status TEXT(20) NOT NULL /* possible values: pending, success, failed, refunded */, type TEXT(20) NOT NULL /* possible values: credit, debit */, description TEXT, coupon_id INTEGER /* NULL if transaction doesn't involve a coupon */, created_at TIMESTAMP NOT NULL, completed_at TIMESTAMP /* NULL if failed */, transaction_ref TEXT(36) NOT NULL /* randomly generated uuid4 for users' reference */, gateway_name TEXT(50),
 gateway_ref TEXT(50),
 device_id TEXT(50),
 ip_address TEXT(50),
 user_agent TEXT);

CREATE TABLE wallet_user_balance_daily (user_id INTEGER, balance REAL(10, 2) NOT NULL, updated_at TIMESTAMP NOT NULL);

CREATE TABLE wallet_merchant_balance_daily (merchant_id INTEGER, balance REAL(10, 2) NOT NULL, updated_at TIMESTAMP NOT NULL);

CREATE TABLE notifications (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL , message TEXT NOT NULL, type TEXT(50) NOT NULL /* possible values: transaction, promotion, security, general */, status TEXT(20) NOT NULL /* possible values: unread, read, archived */, created_at TIMESTAMP NOT NULL, read_at TIMESTAMP /* NULL if not read */, device_type TEXT(10) /* possible values: mobile_app, web_app, email, sms */, device_id TEXT(36),
 action_url TEXT /* can be external https or deeplink url within the app */);

CREATE TABLE user_sessions (user_id INTEGER NOT NULL, session_start_ts TIMESTAMP NOT NULL, session_end_ts TIMESTAMP, device_type TEXT(10) /* possible values: mobile_app, web_app, email, sms */, device_id TEXT(36));

CREATE TABLE user_setting_snapshot (user_id INTEGER NOT NULL, snapshot_date DATE NOT NULL, tx_limit_daily REAL(10, 2),
 tx_limit_monthly REAL(10, 2),
 membership_status INTEGER /* 0 for bronze, 1 for silver, 2 for gold, 3 for platinum, 4 for VIP */, password_hash TEXT(255),
 api_key TEXT(255),
 verified_devices TEXT /* comma separated list of device ids */, verified_ips TEXT /* comma separated list of IP addresses */, mfa_enabled INTEGER, marketing_opt_in INTEGER, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);

INSERT INTO users (uid, username, email, phone_number, created_at, user_type, status, country, address_billing, address_delivery, kyc_status) VALUES
 (1, 'john_doe', 'john.doe@email.com', '+1234567890', DATETIME('now', '-1 month'), 'individual', 'active', 'US', '123 Main St, Anytown US 12345', '123 Main St, Anytown US 12345', 'approved'),
 (2, 'jane_smith', 'jane.smith@email.com', '+9876543210', DATETIME('now', '-2 months'), 'individual', 'active', 'CA', '456 Oak Rd, Toronto ON M1M2M2', '456 Oak Rd, Toronto ON M1M2M2', 'approved'),
 (3, 'bizuser', 'contact@business.co', '+1234509876', '2021-06-01 09:15:00', 'business', 'active', 'FR', '12 Rue Baptiste, Paris 75001', NULL, 'approved'),
 (4, 'david_miller', 'dave@personal.email', '+4477788899', '2023-03-20 18:45:00', 'individual', 'inactive', 'GB', '25 London Road, Manchester M12 4XY', '25 London Road, Manchester M12 4XY', 'pending'),
 (5, 'emily_wilson', 'emily.w@gmail.com', '+8091017161', '2021-11-03 22:10:00', 'individual', 'suspended', 'AU', '72 Collins St, Melbourne VIC 3000', '19 Smith St, Brunswick VIC 3056', 'rejected'),
 (6, 'techcorp', 'orders@techcorp.com', '+14165558888', '2018-05-20 11:35:00', 'business', 'active', 'US', '33 Technology Dr, Silicon Valley CA 94301', NULL, 'approved'),
 (7, 'shopsmart', 'customerserv@shopsmart.biz', '+6585771234', '2020-09-15 06:25:00', 'business', 'inactive', 'SG', '888 Orchard Rd, #05-000, Singapore 238801', NULL, 'approved'),
 (8, 'michael_brown', 'mike.brown@outlook.com', '+3912378624', '2019-07-22 16:40:00', 'individual', 'active', 'DE', 'Heidestr 17, Berlin 10557', 'Heidestr 17, Berlin 10557', 'approved'),
 (9, 'alex_taylor', 'ataylo@university.edu', NULL, '2022-08-30 09:15:00', 'individual', 'active', 'NZ', '12 Mardon Rd, Wellington 6012', '5 Boulcott St, Wellington 6011', 'approved'),
 (10, 'huang2143', 'huang2143@example.com', '+8612345678901', '2023-12-10 08:00:00', 'individual', 'active', 'CN', '123 Nanjing Road, Shanghai 200000', '123 Nanjing Road, Shanghai 200000', 'approved'),
 (11, 'lisa_jones', 'lisa.jones@email.com', '+6123456789', '2023-09-05 15:20:00', 'individual', 'active', 'AU', '789 George St, Sydney NSW 2000', '789 George St, Sydney NSW 2000', 'approved');

INSERT INTO merchants (mid, name, description, website_url, logo_url, created_at, country, state, city, postal_code, address, status, category, sub_category, mcc, contact_name, contact_email, contact_phone) VALUES
 (1, 'TechMart', 'Leading electronics retailer', 'https://www.techmart.com', 'https://www.techmart.com/logo.png', '2015-01-15 00:00:00', 'US', 'California', 'Los Angeles', '90011', '645 Wilshire Blvd, Los Angeles CA 90011', 'active', 'retail (hardware)', 'Electronics', 5732, 'John Jacobs', 'jjacobs@techmart.com', '+15551234567'),
 (2, 'FitLifeGear', 'Fitness equipment and activewear', 'https://fitlifegear.com', 'https://fitlifegear.com/brand.jpg', '2018-07-01 00:00:00', 'CA', 'Ontario', 'Toronto', 'M5V2J2', '421 Richmond St W, Toronto ON M5V2J2', 'active', 'retail (hardware)', 'Sporting Goods', 5655, 'Jane McDonald', 'jmcdonald@fitlifegear.com', '+14165559876'),
 (3, 'UrbanDining', 'Local restaurants and cafes', 'https://www.urbandining.co', 'https://www.urbandining.co/logo.png', '2020-03-10 00:00:00', 'FR', NULL, 'Paris', '75011', '35 Rue du Faubourg Saint-Antoine, 75011 Paris', 'active', 'Food & Dining', 'Restaurants', 5812, 'Pierre Gagnon', 'pgagnon@urbandining.co', '+33612345678'),
 (4, 'LuxStays', 'Boutique vacation rentals', 'https://luxstays.com', 'https://luxstays.com/branding.jpg', '2016-11-01 00:00:00', 'IT', NULL, 'Rome', '00187', 'Via della Conciliazione 15, Roma 00187', 'inactive', 'Travel & Hospitality', 'Accommodation', 7011, 'Marco Rossi', 'mrossi@luxstays.com', '+39061234567'),
 (5, 'HandyCraft', 'Handmade arts and crafts supplies', 'https://handycraft.store', 'https://handycraft.store/hc-logo.png', '2022-06-20 00:00:00', 'ES', 'Catalonia', 'Barcelona', '08003', 'Passeig de Gracia 35, Barcelona 08003', 'active', 'Retail', 'Crafts & Hobbies', 5949, 'Ana Garcia', 'agarcia@handycraft.store', '+34612345678'),
 (6, 'CodeSuite', 'SaaS productivity tools for developers', 'https://codesuite.io', 'https://codesuite.io/logo.svg', '2019-02-01 00:00:00', 'DE', NULL, 'Berlin', '10119', 'Dessauer Str 28, 10119 Berlin', 'active', 'Business Services', 'Software', 5734, 'Michael Schmidt', 'mschmidt@codesuite.io', '+49301234567'),
 (7, 'ZenHomeGoods', 'Housewares and home decor items', 'https://www.zenhomegoods.com', 'https://www.zenhomegoods.com/branding.jpg', '2014-09-15 00:00:00', 'AU', 'Victoria', 'Melbourne', '3004', '159 Franklin St, Melbourne VIC 3004', 'active', 'Retail', 'Home & Garden', 5719, 'Emily Watson', 'ewatson@zenhomegoods.com', '+61312345678'),
 (8, 'KidzPlayhouse', 'Children''s toys and games', 'https://kidzplayhouse.com', 'https://kidzplayhouse.com/logo.png', '2017-04-01 00:00:00', 'GB', NULL, 'London', 'WC2N 5DU', '119 Charing Cross Rd, London WC2N 5DU', 'suspended', 'Retail', 'Toys & Games', 5945, 'David Thompson', 'dthompson@kidzplayhouse.com', '+442071234567'),
 (9, 'BeautyTrending', 'Cosmetics and beauty supplies', 'https://beautytrending.com', 'https://beautytrending.com/bt-logo.svg', '2021-10-15 00:00:00', 'NZ', NULL, 'Auckland', '1010', '129 Queen St, Auckland 1010', 'active', 'Retail', 'Health & Beauty', 5977, 'Sophie Wilson', 'swilson@beautytrending.com', '+6493012345'),
 (10, 'GameRush', 'Video games and gaming accessories', 'https://gamerush.co', 'https://gamerush.co/gr-logo.png', '2023-02-01 00:00:00', 'US', 'New York', 'New York', '10001', '303 Park Ave S, New York NY 10001', 'active', 'Retail', 'Electronics', 5735, 'Michael Davis', 'mdavis@gamerush.co', '+16463012345'),
 (11, 'FashionTrend', 'Trendy clothing and accessories', 'https://www.fashiontrend.com', 'https://www.fashiontrend.com/logo.png', '2019-08-10 00:00:00', 'UK', NULL, 'Manchester', 'M2 4WU', '87 Deansgate, Manchester M2 4WU', 'active', 'Retail', 'Apparel', 5651, 'Emma Thompson', 'ethompson@fashiontrend.com', '+441612345678'),
 (12, 'GreenGourmet', 'Organic foods and natural products', 'https://www.greengourmet.com', 'https://www.greengourmet.com/logo.jpg', '2020-12-05 00:00:00', 'CA', 'British Columbia', 'Vancouver', 'V6B 6B1', '850 W Hastings St, Vancouver BC V6B 6B1', 'active', 'Food & Dining', 'Groceries', 5411, 'Daniel Lee', 'dlee@greengourmet.com', '+16041234567'),
 (13, 'PetParadise', 'Pet supplies and accessories', 'https://petparadise.com', 'https://petparadise.com/logo.png', '2018-03-20 00:00:00', 'AU', 'New South Wales', 'Sydney', '2000', '275 Pitt St, Sydney NSW 2000', 'active', 'Retail', 'Pets', 5995, 'Olivia Johnson', 'ojohnson@petparadise.com', '+61298765432'),
 (14, 'HomeTechSolutions', 'Smart home devices and gadgets', 'https://hometechsolutions.net', 'https://hometechsolutions.net/logo.png', '2022-04-15 00:00:00', 'US', 'California', 'San Francisco', '94105', '350 Mission St, San Francisco CA 94105', 'active', 'Retail', 'Home Appliances', 5734, 'Ethan Brown', 'ebrown@hometechsolutions.net', '+14159876543'),
 (15, 'BookWorms', 'Books and reading accessories', 'https://bookworms.co.uk', 'https://bookworms.co.uk/logo.png', '2017-06-30 00:00:00', 'UK', NULL, 'London', 'WC2H 9JA', '66-67 Tottenham Court Rd, London WC2H 9JA', 'active', 'Retail', 'Books', 5942, 'Sophia Turner', 'sturner@bookworms.co.uk', '+442078912345');

INSERT INTO coupons (cid, merchant_id, code, description, start_date, end_date, discount_type, discount_value, min_purchase_amount, max_discount_amount, redemption_limit, status, created_at, updated_at) VALUES
 (1, 1, 'TECH20', '20% off tech and electronics', '2023-05-01', '2023-05-31', 'percentage', 20.00, 100.00, NULL, 500, 'active', '2023-04-01 09:00:00', '2023-04-15 11:30:00'),
 (2, 2, 'NEWYEAR30', '30% off workout gear', '2023-01-01', '2023-01-15', 'percentage', 30.00, NULL, NULL, 1000, 'expired', '2022-12-01 12:00:00', '2023-01-16 18:45:00'),
 (3, 3, 'DINEDISCOUNT', 'Get $10 off $50 order', '2023-06-01', '2023-06-30', 'fixed_amount', 10.00, 50.00, 10.00, NULL, 'active', '2023-05-15 15:30:00', NULL),
 (4, 4, 'HOME15', '15% off weekly rental', '2023-07-01', '2023-08-31', 'percentage', 15.00, 1000.00, 300.00, 200, 'active', '2023-05-01 09:15:00', NULL),
 (5, 5, 'HOME10', '$10 off $75+ purchase', '2023-04-01', '2023-04-30', 'fixed_amount', 10.00, 75.00, 10.00, 300, 'inactive', '2023-03-01 14:00:00', '2023-05-05 10:30:00'),
 (6, 6, 'CODENEW25', '25% off new subscriptions', '2023-03-01', '2023-03-31', 'percentage', 25.00, NULL, NULL, NULL, 'expired', '2023-02-15 11:00:00', '2023-04-01 09:30:00'),
 (7, 7, 'ZENHOME', 'Get 20% off home items', '2023-09-01', '2023-09-30', 'percentage', 20.00, 50.00, NULL, 1500, 'active', '2023-08-15 16:45:00', NULL),
 (8, 8, 'GAMEKIDS', '$15 off $100+ purchase', '2022-12-01', '2022-12-31', 'fixed_amount', 15.00, 100.00, 15.00, 800, 'expired', '2022-11-01 10:30:00', '2023-01-02 13:15:00'),
 (9, 9, 'GLOWUP', 'Buy 2 get 1 free on cosmetics', '2023-10-15', '2023-10-31', 'fixed_amount', 50.00, 150.00, 50.00, 300, 'active', '2023-10-01 08:00:00', NULL),
 (10, 10, 'GAMERALERT', 'Get 25% off accessories', '2023-03-01', '2023-03-15', 'percentage', 25.00, NULL, 50.00, 750, 'expired', '2023-02-15 14:30:00', '2023-03-16 12:00:00');

INSERT INTO wallet_transactions_daily (txid, sender_id, sender_type, receiver_id, receiver_type, amount, status, type, description, coupon_id, created_at, completed_at, transaction_ref, gateway_name, gateway_ref, device_id, ip_address, user_agent) VALUES
 (1, 1, 0, 1, 0, 99.99, 'success', 'debit', 'Online purchase', NULL, '2023-06-01 10:15:30', '2023-06-01 10:15:45', 'ad154bf7-8185-4230-a8d8-3ef59b4e0012', 'Stripe', 'tx_123abc456def', 'mobile_8fh2k1', '192.168.0.1', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_3_1 like Mac OS X) ...'),
 (2, 1, 0, 1, 1, 20.00, 'success', 'credit', 'Coupon discount', 1, '2023-06-01 10:15:30', '2023-06-01 10:15:45', 'ad154bf7-8185-4230-a8d8-3ef59b4e0012', 'Stripe', 'tx_123abc456def', 'mobile_8fh2k1', '192.168.0.1', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_3_1 like Mac OS X) ...'),
 (3, 2, 0, 1, 1, 16.00, 'success', 'credit', 'Coupon discount', 1, '2023-07-01 10:18:30', '2023-06-01 10:18:45', 'kd454bf7-428d-eig2-a8d8-3ef59b4e0012', 'Stripe', 'tx_123abc789gas', 'mobile_yjp08q', '198.51.100.233', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) ...'),
 (4, 3, 1, 9, 0, 125.50, 'success', 'debit', 'Product purchase', NULL, '2023-06-01 13:22:18', '2023-06-01 13:22:45', 'e6f510e9-ff7d-4914-81c2-f8e56bae4012', 'PayPal', 'ppx_192ks8hl', 'web_k29qjd', '216.58.195.68', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ...'),
 (5, 9, 0, 3, 1, 42.75, 'pending', 'debit', 'Order #438721', 3, '2023-06-01 18:45:02', '2023-06-01 18:45:13', 'b2ca190e-a42f-4f5e-8318-f82bcc6ae64e', 'Stripe', 'tx_987zyx654wvu', 'mobile_q3mz8n', '68.85.32.201', 'Mozilla/5.0 (Linux; Android 13) ...'),
 (6, 9, 0, 3, 1, 10.00, 'success', 'credit', 'Coupon discount', 3, '2023-06-01 18:45:02', '2023-06-01 18:45:13', 'b2ca190e-a42f-4f5e-8318-f82bcc6ae64e', 'Stripe', 'tx_987zyx654wvu', 'mobile_q3mz8n', '68.85.32.201', 'Mozilla/5.0 (Linux; Android 13) ...'),
 (7, 2, 0, 7, 1, 89.99, 'pending', 'debit', 'Home furnishings', NULL, '2023-06-02 09:30:25', '2023-06-02 09:30:40', 'c51e10d1-db34-4d9f-b55f-43a05a5481c8', 'Checkout.com', 'ord_kzhg123', 'mobile_yjp08q', '198.51.100.233', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) ...'),
 (8, 2, 0, 7, 1, 17.99, 'success', 'credit', 'Coupon discount', 7, '2023-06-02 09:30:25', '2023-06-02 09:30:40', 'c51e10d1-db34-4d9f-b55f-43a05a5481c8', 'Checkout.com', 'ord_kzhg123', 'mobile_yjp08q', '198.51.100.233', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) ...'),
 (9, 6, 1, 1, 0, 29.95, 'success', 'debit', 'Software subscription', NULL, '2023-06-02 14:15:00', '2023-06-02 14:15:05', '25cd48e5-08c3-4d1c-b7a4-26485ea646eb', 'Braintree', 'sub_mnb456', 'web_zz91p44l', '4.14.15.90', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...'),
 (10, 4, 0, 4, 1, 2500.00, 'pending', 'debit', 'Villa rental deposit', NULL, '2023-06-02 20:45:36', NULL, 'a7659c81-0cd0-4635-af6c-cf68d2c15ab2', 'PayPal', NULL, 'mobile_34jdkl', '143.92.64.138', 'Mozilla/5.0 (Linux; Android 11; Pixel 5) ...'),
 (11, 5, 0, 5, 1, 55.99, 'success', 'debit', 'Craft supplies order', NULL, '2023-06-03 11:12:20', '2023-06-03 11:12:35', 'ec74cb3b-8272-4175-a5d0-f03c2e781593', 'Adyen', 'ord_tkjs87', 'web_8902wknz', '192.64.112.188', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ...'),
 (12, 9, 0, 9, 1, 75.00, 'success', 'debit', 'Beauty products', 9, '2023-06-04 08:00:00', '2023-06-04 08:00:25', '840a9854-1b07-422b-853c-636b289222a9', 'Checkout.com', 'ord_kio645', 'mobile_g3mjfz', '203.96.81.36', 'Mozilla/5.0 (Linux; Android 12; SM-S906N Build/QP1A.190711.020) ...'),
 (13, 9, 0, 9, 1, 50.00, 'success', 'credit', 'Coupon discount', 9, '2023-06-04 08:00:00', '2023-06-04 08:00:25', '840a9854-1b07-422b-853c-636b289222a9', 'Checkout.com', 'ord_kio645', 'mobile_g3mjfz', '203.96.81.36', 'Mozilla/5.0 (Linux; Android 12; SM-S906N Build/QP1A.190711.020) ...'),
 (14, 8, 0, 10, 1, 119.99, 'failed', 'debit', 'New game purchase', NULL, '2023-06-04 19:30:45', NULL, '32e2b29c-5c7f-4906-98c5-e8abdcbfd69a', 'Braintree', 'ord_mjs337', 'web_d8180kaf', '8.26.53.165', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...'),
 (15, 8, 0, 10, 1, 29.99, 'success', 'credit', 'Coupon discount', 10, '2023-06-04 19:30:45', '2023-06-04 19:31:10', '32e2b29c-5c7f-4906-98c5-e8abdcbfd69a', 'Braintree', 'ord_mjs337', 'web_d8180kaf', '8.26.53.165', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...'),
 (16, 10, 1, 3, 0, 87.50, 'failed', 'debit', 'Restaurant order', NULL, '2023-06-05 12:05:21', NULL, '37cf052d-0475-4ecc-bda7-73ee904bf65c', 'Checkout.com', NULL, 'mobile_x28qlj', '92.110.51.150', 'Mozilla/5.0 (Linux; Android 13; SM-S901B) ...'),
 (17, 1, 0, 1, 0, 175.00, 'success', 'debit', 'Refund on order #1234', NULL, '2023-06-06 14:20:00', '2023-06-06 14:20:05', 'a331232e-a3f6-4e7f-b49f-3588bc5ff985', 'Stripe', 'rfnd_xkt521', 'web_33lq1dh', '38.75.197.8', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ...'),
 (18, 7, 1, 2, 0, 599.99, 'success', 'debit', 'Yearly subscription', NULL, '2023-06-06 16:55:10', '2023-06-06 16:55:15', 'ed6f46ab-9617-4d11-9aa9-60d24bdf9bc0', 'PayPal', 'sub_pjj908', 'web_zld22f', '199.59.148.201', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ...'),
 (19, 2, 0, 2, 1, 22.99, 'refunded', 'debit', 'Product return', NULL, '2023-06-07 10:10:30', '2023-06-07 10:11:05', '6c97a87d-610f-4705-ae97-55071127d9ad', 'Adyen', 'tx_zcx258', 'mobile_1av8p0', '70.121.39.25', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) ...'),
 (20, 2, 0, 2, 1, 22.99, 'success', 'credit', 'Refund on return', NULL, '2023-06-07 10:10:30', '2023-06-07 10:11:05', '6c97a87d-610f-4705-ae97-55071127d9ad', 'Adyen', 'tx_zcx258', 'mobile_1av8p0', '70.121.39.25', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) ...'),
 (21, 1, 0, 2, 1, 49.99, 'success', 'debit', 'Product purchase', NULL, DATETIME('now', '-5 months'), DATETIME('now', '-5 months'), 'tx_ref_11_1', 'Stripe', 'stripe_ref_11_1', 'device_11_1', '192.168.1.11', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36'),
 (22, 4, 0, 3, 1, 99.99, 'success', 'debit', 'Service purchase', NULL, DATETIME('now', '-4 months'), DATETIME('now', '-4 months'), 'tx_ref_12_1', 'PayPal', 'paypal_ref_12_1', 'device_12_1', '192.168.1.12', 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1'),
 (23, 4, 0, 1, 1, 149.99, 'success', 'debit', 'Subscription purchase', NULL, DATETIME('now', '-3 months'), DATETIME('now', '-3 months'), 'tx_ref_13_1', 'Stripe', 'stripe_ref_13_1', 'device_13_1', '192.168.1.13', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36'),
 (24, 2, 0, 5, 1, 199.99, 'pending', 'debit', 'Product purchase', NULL, DATETIME('now', '-2 months'), DATETIME('now', '-2 months'), 'tx_ref_14_1', 'PayPal', 'paypal_ref_14_1', 'device_14_1', '192.168.1.14', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0'),
 (25, 2, 0, 1, 1, 249.99, 'success', 'debit', 'Service purchase', NULL, DATETIME('now', '-1 month'), DATETIME('now', '-1 month'), 'tx_ref_15_1', 'Stripe', 'stripe_ref_15_1', 'device_15_1', '192.168.1.15', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36'),
 (26, 7, 1, 2, 0, 299.99, 'success', 'debit', 'Renew subscription', NULL, DATETIME('now', '-21 days'), DATETIME('now', '-21 days'), 'ed6f46ab-9617-4d11-9aa9-55071127d9ad', 'PayPal', 'sub_pjk832', 'web_zld22f', '199.59.148.201', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ...');

INSERT INTO wallet_user_balance_daily (user_id, balance, updated_at) VALUES
 (1, 525.80, '2023-06-07 23:59:59'),
 (2, 429.76, '2023-06-07 23:59:59'),
 (3, -725.55, '2023-06-07 23:59:59'),
 (4, -2500.00, '2023-06-07 23:59:59'),
 (5, -55.99, '2023-06-07 23:59:59'),
 (6, 0.00, '2023-06-07 23:59:59'),
 (7, 0.00, '2023-06-07 23:59:59'),
 (8, -599.98, '2023-06-07 23:59:59'),
 (9, -183.25, '2023-06-07 23:59:59'),
 (10, 0.00, '2023-06-07 23:59:59'),
 (1, 2739.10, DATETIME('now', '-8 days')),
 (1, 2738.12, DATETIME('now', '-6 days')),
 (1, 2733.92, DATETIME('now', '-3 days')),
 (2, 155.24, DATETIME('now', '-7 days')),
 (3, 2775.25, DATETIME('now', '-6 days')),
 (4, 2500.00, DATETIME('now', '-5 days')),
 (5, 155.99, DATETIME('now', '-4 days')),
 (6, 29.95, DATETIME('now', '-3 days')),
 (7, 172.98, DATETIME('now', '-2 days')),
 (8, 0.00, DATETIME('now', '-7 days')),
 (9, 125.00, DATETIME('now', '-3 days')),
 (10, 219.98, DATETIME('now', '-1 days'));

INSERT INTO wallet_merchant_balance_daily (merchant_id, balance, updated_at) VALUES
 (1, 3897.99, '2023-06-07 23:59:59'),
 (2, 155.24, '2023-06-07 23:59:59'),
 (3, 2775.25, '2023-06-07 23:59:59'),
 (4, 2500.00, '2023-06-07 23:59:59'),
 (5, 155.99, '2023-06-07 23:59:59'),
 (6, 29.95, '2023-06-07 23:59:59'),
 (7, 172.98, '2023-06-07 23:59:59'),
 (8, 0.00, '2023-06-07 23:59:59'),
 (9, 125.00, '2023-06-07 23:59:59'),
 (10, 219.98, '2023-06-07 23:59:59'),
 (1, 82.10, DATE('now', '-8 days')),
 (2, 82.12, DATE('now', '-8 days')),
 (1, 82.92, DATE('now', '-7 days')),
 (2, 55.24, DATE('now', '-7 days')),
 (3, 75.25, DATE('now', '-7 days')),
 (1, 50.00, DATE('now')),
 (2, 55.99, DATE('now')),
 (3, 29.95, DATE('now')),
 (4, 89.99, DATE('now')),
 (5, 599.99, DATE('now'));

INSERT INTO notifications (id, user_id, message, type, status, created_at, device_type, device_id, action_url) VALUES
 (1, 1, 'Your order #123abc has been shipped!', 'transaction', 'unread', '2023-06-01 10:16:00', 'mobile_app', 'mobile_8fh2k1', 'app://orders/123abc'),
 (2, 1, 'Get 20% off your next purchase! Limited time offer.', 'promotion', 'unread', '2023-06-02 09:00:00', 'email', NULL, 'https://techmart.com/promo/TECH20'),
 (3, 2, 'A package is being returned to you. Refund processing...', 'transaction', 'read', '2023-06-07 10:12:00', 'mobile_app', 'mobile_1av8p0', 'app://orders?status=returned'),
 (4, 2, 'Your FitLife membership is up for renewal on 7/1', 'general', 'unread', '2023-06-05 15:30:00', 'email', NULL, 'https://fitlifegear.com/renew'),
 (5, 3, 'An order from UrbanDining was unsuccessful', 'transaction', 'read', '2023-06-05 12:06:00', 'sms', NULL, 'https://urbandining.co/orders/37cf052d'),
 (6, 4, 'Your rental request is pending approval', 'transaction', 'unread', '2023-06-02 20:46:00', 'mobile_app', 'mobile_34jdkl', 'app://bookings/a7659c81'),
 (7, 5, 'Claim your 25% discount on craft supplies!', 'promotion', 'archived', '2023-06-01 08:00:00', 'email', NULL, 'https://handycraft.store/CRAFTY10'),
 (8, 6, 'Your CodeSuite subscription will renew on 7/1', 'general', 'unread', '2023-06-01 12:00:00', 'email', NULL, 'https://codesuite.io/subscriptions'),
 (9, 7, 'Thanks for shopping at ZenHomeGoods! How did we do?', 'general', 'read', '2023-06-02 09:31:00', 'mobile_app', 'mobile_yjp08q', 'https://zenhomesurvey.com/order/c51e10d1'),
 (10, 8, 'Playtime! New games and toys have arrived', 'promotion', 'archived', '2023-06-01 18:00:00', 'email', NULL, 'https://kidzplayhouse.com/new-arrivals'),
 (11, 9, 'Here''s $10 to start your glow up!', 'promotion', 'unread', '2023-06-01 10:15:00', 'email', NULL, 'https://beautytrending.com/new-customer'),
 (12, 10, 'Your order #ord_mjs337 is being processed', 'transaction', 'read', '2023-06-04 19:31:30', 'web_app', 'web_d8180kaf', 'https://gamerush.co/orders/32e2b29c'),
 (13, 1, 'New promotion: Get 10% off your next order!', 'promotion', 'unread', DATETIME('now', '-7 days'), 'email', NULL, 'https://techmart.com/promo/TECH10'),
 (14, 1, 'Your order #456def has been delivered', 'transaction', 'unread', DATETIME('now', '-14 days'), 'mobile_app', 'mobile_8fh2k1', 'app://orders/456def'),
 (15, 2, 'Reminder: Your FitLife membership expires in 7 days', 'general', 'unread', DATETIME('now', '-21 days'), 'email', NULL, 'https://fitlifegear.com/renew'),
 (16, 2, 'Weekend Flash Sale: 25% off all activewear!', 'promotion', 'unread', DATETIME('now', '-7 days', '+2 day'), 'mobile_app', 'mobile_yjp08q', 'app://shop/activewear');

INSERT INTO user_sessions (user_id, session_start_ts, session_end_ts, device_type, device_id) VALUES
 (1, '2023-06-01 09:45:22', '2023-06-01 10:20:35', 'mobile_app', 'mobile_8fh2k1'),
 (1, '2023-06-02 13:30:00', '2023-06-02 14:15:15', 'web_app', 'web_33lq1dh'),
 (1, '2023-06-06 14:19:00', '2023-06-06 14:22:10', 'web_app', 'web_33lq1dh'),
 (1, '2023-06-07 23:49:12', '2023-06-08 00:00:00', 'web_app', 'web_33lq1dh'),
 (2, '2023-06-02 08:55:08', '2023-06-02 09:45:42', 'mobile_app', 'mobile_yjp08q'),
 (2, '2023-06-07 10:09:15', '2023-06-07 10:12:25', 'mobile_app', 'mobile_1av8p0'),
 (3, '2023-06-01 13:15:33', '2023-06-01 13:28:01', 'web_app', 'web_k29qjd'),
 (3, '2023-06-05 12:00:00', '2023-06-05 12:10:22', 'mobile_app', 'mobile_x28qlj'),
 (4, '2023-06-02 20:30:12', '2023-06-02 21:15:48', 'mobile_app', 'mobile_34jdkl'),
 (5, '2023-06-03 10:45:30', '2023-06-03 11:20:28', 'web_app', 'web_8902wknz'),
 (6, '2023-06-02 14:00:00', '2023-06-02 15:10:05', 'web_app', 'web_zz91p44l'),
 (7, '2023-06-06 16:45:22', '2023-06-06 17:10:40', 'web_app', 'web_zld22f'),
 (8, '2023-06-04 19:25:15', '2023-06-04 19:40:20', 'web_app', 'web_d8180kaf'),
 (8, '2023-06-01 17:30:00', '2023-06-01 18:15:35', 'mobile_app', 'mobile_q3mz8n'),
 (9, '2023-06-04 07:45:30', '2023-06-04 08:15:27', 'mobile_app', 'mobile_g3mjfz'),
 (10, '2023-06-02 14:10:15', '2023-06-02 14:40:58', 'web_app', 'web_zz91p44l'),
 (5, DATETIME('now', '-32 days'), DATETIME('now', '-32 days', '+15 minute'), 'web_app', 'web_8902wknz'),
 (6, DATETIME('now', '-8 days'), DATETIME('now', '-8 days', '+15 minute'), 'web_app', 'web_zz91p44l'),
 (7, DATETIME('now', '-5 days'), DATETIME('now', '-5 days', '+15 minute'), 'web_app', 'web_zz91p44l'),
 (8, DATETIME('now', '-3 days'), DATETIME('now', '-3 days', '+15 minute'), 'web_app', 'web_d8180kaf'),
 (9, DATETIME('now', '-1 days'), DATETIME('now', '-1 days', '+15 minute'), 'mobile_app', 'mobile_g3mjfz'),
 (10, DATETIME('now', '-2 days'), DATETIME('now', '-2 days', '+15 minute'), 'web_app', 'web_zz91p44l'),
 (5, DATETIME('now', '-2 days'), DATETIME('now', '-2 days', '+15 minute'), 'web_app', 'web_8902wknz');

INSERT INTO user_setting_snapshot (user_id, snapshot_date, tx_limit_daily, tx_limit_monthly, membership_status, password_hash, api_key, verified_devices, verified_ips, mfa_enabled, marketing_opt_in, created_at) VALUES
 (1, '2023-06-07', 1000.00, 5000.00, 2, 'bcryptHash($2yz9!&ka1)', '9d61c49b-8977-4914-a36b-80d1445e38fa', 'mobile_8fh2k1', '192.168.0.1', TRUE, FALSE, '2023-06-07 00:00:00'),
 (2, '2023-06-07', 500.00, 2500.00, 1, 'bcryptHash(qpwo9874zyGk!)', NULL, 'mobile_yjp08q, mobile_1av8p0', '198.51.100.233, 70.121.39.25', FALSE, TRUE, '2023-06-07 00:00:00'),
 (3, '2023-06-07', 2000.00, 10000.00, 3, 'bcryptHash(Fr3nchPa1n!@98zy)', 'e785f611-fdd8-4c2d-a870-e104358712e5', 'web_k29qjd, mobile_x28qlj', '216.58.195.68, 92.110.51.150', TRUE, FALSE, '2023-06-07 00:00:00'),
 (4, '2023-06-07', 5000.00, 20000.00, 4, 'bcryptHash(Vacay2023*&!Rm)', NULL, 'mobile_34jdkl', '143.92.64.138', FALSE, TRUE, '2023-06-07 00:00:00'),
 (5, '2023-06-07', 100.00, 500.00, 0, 'bcryptHash(cRaf7yCr8zy)', NULL, 'web_8902wknz', '192.64.112.188', FALSE, FALSE, '2023-06-07 00:00:00'),
 (6, '2023-06-07', 50.00, 500.00, 1, 'bcryptHash(C0d3Rul3z!99)', '6c03c175-9ac9-4854-b064-a3fff2c62e31', 'web_zz91p44l', '4.14.15.90', TRUE, TRUE, '2023-06-07 00:00:00'),
 (7, '2023-06-07', 250.00, 1000.00, 2, 'bcryptHash(zEnH0me&Pw7)', NULL, NULL, NULL, FALSE, TRUE, '2023-06-07 00:00:00'),
 (8, '2023-06-07', 200.00, 1000.00, 0, 'bcryptHash(K1dzPlay!&Rt8)', NULL, 'web_d8180kaf, mobile_q3mz8n', '8.26.53.165, 68.85.32.201', FALSE, FALSE, '2023-06-07 00:00:00'),
 (9, '2023-06-07', 150.00, 1000.00, 2, 'bcryptHash(Gl0wUp7!9zy)', NULL, 'mobile_g3mjfz', '203.96.81.36', TRUE, TRUE, '2023-06-07 00:00:00'),
 (10, '2023-06-07', 300.00, 2000.00, 1, 'bcryptHash(GamzRu1ez*&99!)', NULL, 'web_d8180kaf', '8.26.53.165', FALSE, TRUE, '2023-06-07 00:00:00'),
 (1, '2023-06-01', 502.00, 1000.00, 2, 'bcryptHash($2yz9!&ka1)', '9d61c49b-8977-4914-a36b-80d1445e38fa', 'mobile_8fh2k1', '192.168.0.1', FALSE, TRUE, '2023-06-01 06:00:00'),
 (2, '2023-06-01', 500.00, 2500.00, 1, 'bcryptHash(qpwo9874zyGk!)', NULL, 'mobile_yjp08q', '198.51.100.233, 70.121.39.25', TRUE, FALSE, '2023-06-01 09:00:00');