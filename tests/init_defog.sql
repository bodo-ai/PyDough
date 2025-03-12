-- Modified version of the defog.ai schema setup for the broker schema:
-- https://github.com/defog-ai/defog-data/blob/main/defog_data/broker/broker.sql
-- changed slightly to use SQLite dialect instead of postgress.

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
