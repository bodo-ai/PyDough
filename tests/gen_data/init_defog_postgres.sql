-- Create schemas
CREATE SCHEMA IF NOT EXISTS main;


-- Dimension tables
DROP TABLE IF EXISTS main.sbCustomer CASCADE;
CREATE TABLE main.sbCustomer (
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

DROP TABLE IF EXISTS main.sbTicker CASCADE;
CREATE TABLE main.sbTicker (
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
DROP TABLE IF EXISTS main.sbDailyPrice CASCADE;
CREATE TABLE main.sbDailyPrice (
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

DROP TABLE IF EXISTS main.sbTransaction CASCADE;
CREATE TABLE main.sbTransaction (
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
INSERT INTO main.sbCustomer (sbCustId, sbCustName, sbCustEmail, sbCustPhone, sbCustAddress1, sbCustCity, sbCustState, sbCustCountry, sbCustPostalCode, sbCustJoinDate, sbCustStatus) VALUES
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
('C015', 'Emma Brown', 'emma.brown@email.com', '555-987-6543', '789 Oak St', 'Newtown', 'NJ', 'USA', '08801', DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months', 'active'),
('C016', 'sophia martinez', 'sophia.martinez@email.com', '555-246-8135', '159 Elm Ave', 'Anytown', 'CA', 'USA', '90001', DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '4 months', 'active'),
('C017', 'Jacob Taylor', 'jacob.taylor@email.com', '555-135-7902', '753 Pine Rd', 'Someville', 'NY', 'USA', '10002', DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 months', 'active'),
('C018', 'Michael Anderson', 'michael.anderson@email.com', '555-864-2319', '321 Cedar Ln', 'Yourtown', 'CA', 'USA', '92101', DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2 months', 'active'),
('C019', 'Isabella Thompson', 'isabella.thompson@email.com', '555-753-1904', '987 Maple Dr', 'Anothertown', 'FL', 'USA', '33101', DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month', 'active'),
('C020', 'Maurice Lee', 'maurice.lee@email.com', '555-370-2648', '654 Oak St', 'Mytown', 'TX', 'USA', '75000', DATE_TRUNC('month', CURRENT_DATE), 'active');


-- sbTicker  
INSERT INTO main.sbTicker (sbTickerId, sbTickerSymbol, sbTickerName, sbTickerType, sbTickerExchange, sbTickerCurrency, sbTickerDb2x, sbTickerIsActive) VALUES
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
INSERT INTO main.sbDailyPrice (sbDpTickerId, sbDpDate, sbDpOpen, sbDpHigh, sbDpLow, sbDpClose, sbDpVolume, sbDpEpochMs, sbDpSource) VALUES
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
('T019', CURRENT_DATE - INTERVAL '8 days', 204.00, 204.50, 202.75, 203.25, 8000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '8 days') * 1000, 'NYSE'),
('T019', CURRENT_DATE - INTERVAL '6 days', 205.00, 207.50, 203.75, 206.25, 8000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '6 days') * 1000, 'NYSE'),
('T019', CURRENT_DATE - INTERVAL '5 days', 206.50, 208.00, 205.00, 207.00, 7500000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '5 days') * 1000, 'NYSE'),
('T019', CURRENT_DATE - INTERVAL '4 days', 207.25, 209.00, 206.50, 208.50, 7000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '4 days') * 1000, 'NYSE'),
('T019', CURRENT_DATE - INTERVAL '3 days', 208.75, 210.50, 207.75, 209.75, 6500000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '3 days') * 1000, 'NYSE'),
('T019', CURRENT_DATE - INTERVAL '2 days', 210.00, 211.75, 209.25, 211.00, 6000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '2 days') * 1000, 'NYSE'),
('T019', CURRENT_DATE - INTERVAL '1 day', 211.25, 213.00, 210.50, 212.25, 5500000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '1 day') * 1000, 'NYSE'),
('T019', CURRENT_DATE, 212.50, 214.25, 211.75, 213.50, 5000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000, 'NYSE'),
('T020', CURRENT_DATE - INTERVAL '6 days', 82.00, 83.00, 81.50, 82.50, 1000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '6 days') * 1000, 'Vanguard'),  
('T020', CURRENT_DATE - INTERVAL '5 days', 82.60, 83.60, 82.10, 83.10, 950000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '5 days') * 1000, 'Vanguard'),
('T020', CURRENT_DATE - INTERVAL '4 days', 83.20, 84.20, 82.70, 83.70, 900000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '4 days') * 1000, 'Vanguard'),  
('T020', CURRENT_DATE - INTERVAL '3 days', 83.80, 84.80, 83.30, 84.30, 850000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '3 days') * 1000, 'Vanguard'),
('T020', CURRENT_DATE - INTERVAL '2 days', 84.40, 85.40, 83.90, 84.90, 800000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '2 days') * 1000, 'Vanguard'),
('T020', CURRENT_DATE - INTERVAL '1 day', 85.00, 86.00, 84.50, 85.50, 750000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '1 day') * 1000, 'Vanguard'),  
('T020', CURRENT_DATE, 85.60, 86.60, 85.10, 86.10, 700000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000, 'Vanguard'),
('T021', CURRENT_DATE - INTERVAL '6 days', 300.00, 305.00, 297.50, 302.50, 10000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '6 days') * 1000, 'NASDAQ'),
('T021', CURRENT_DATE - INTERVAL '5 days', 303.00, 308.00, 300.50, 305.50, 9500000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '5 days') * 1000, 'NASDAQ'),  
('T021', CURRENT_DATE - INTERVAL '4 days', 306.00, 311.00, 303.50, 308.50, 9000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '4 days') * 1000, 'NASDAQ'),
('T021', CURRENT_DATE - INTERVAL '3 days', 309.00, 314.00, 306.50, 311.50, 8500000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '3 days') * 1000, 'NASDAQ'),
('T021', CURRENT_DATE - INTERVAL '2 days', 312.00, 317.00, 309.50, 314.50, 8000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '2 days') * 1000, 'NASDAQ'),  
('T021', CURRENT_DATE - INTERVAL '1 day', 315.00, 320.00, 312.50, 317.50, 7500000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - INTERVAL '1 day') * 1000, 'NASDAQ'),
('T021', CURRENT_DATE, 318.00, 323.00, 315.50, 320.50, 7000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000, 'NASDAQ');

-- sbTransaction
INSERT INTO main.sbTransaction (sbTxId, sbTxCustId, sbTxTickerId, sbTxDateTime, sbTxType, sbTxShares, sbTxPrice, sbTxAmount, sbTxCcy, sbTxTax, sbTxCommission, sbTxKpx, sbTxSettlementDateStr, sbTxStatus) VALUES
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
('TX031', 'C001', 'T001', NOW() - INTERVAL '9 days', 'buy', 100, 150.00, 15000.00, 'USD', 75.00, 10.00, 'KP031', NULL, 'fail'),
('TX032', 'C002', 'T002', NOW() - INTERVAL '8 days', 'sell', 80, 280.00, 14000.00, 'USD', 70.00, 10.00, 'KP032', TO_CHAR(NOW() - INTERVAL '8 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX033', 'C003', 'T001', NOW() - INTERVAL '7 days', 'buy', 120, 200.00, 24000.00, 'USD', 120.00, 15.00, 'KP033', TO_CHAR(NOW() - INTERVAL '7 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX034', 'C004', 'T004', NOW() - INTERVAL '6 days', 'sell', 90, 320.00, 28800.00, 'USD', 144.00, 12.00, 'KP034', TO_CHAR(NOW() - INTERVAL '6 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX035', 'C005', 'T001', NOW() - INTERVAL '5 days', 'buy', 150, 180.00, 27000.00, 'USD', 135.00, 20.00, 'KP035', NULL, 'fail'),
('TX036', 'C006', 'T006', NOW() - INTERVAL '4 days', 'sell', 70, 300.00, 21000.00, 'USD', 105.00, 15.00, 'KP036', TO_CHAR(NOW() - INTERVAL '4 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX037', 'C007', 'T007', NOW() - INTERVAL '3 days', 'buy', 110, 220.00, 24200.00, 'USD', 121.00, 10.00, 'KP037', TO_CHAR(NOW() - INTERVAL '3 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX038', 'C008', 'T008', NOW() - INTERVAL '2 days', 'sell', 100, 350.00, 35000.00, 'USD', 175.00, 25.00, 'KP038', TO_CHAR(NOW() - INTERVAL '2 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX039', 'C009', 'T007', NOW() - INTERVAL '1 day', 'buy', 80, 230.00, 18400.00, 'USD', 92.00, 18.00, 'KP039', NULL, 'pending'),
('TX040', 'C001', 'T011', NOW() - INTERVAL '10 days', 'buy', 50, 400.00, 20000.00, 'USD', 100.00, 20.00, 'KP040', TO_CHAR(NOW() - INTERVAL '10 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX041', 'C002', 'T012', NOW() - INTERVAL '9 days', 'sell', 30, 320.00, 9600.00, 'USD', 48.00, 15.00, 'KP041', TO_CHAR(NOW() - INTERVAL '9 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX042', 'C003', 'T013', NOW() - INTERVAL '8 days', 'buy', 80, 180.00, 14400.00, 'USD', 72.00, 10.00, 'KP042', TO_CHAR(NOW() - INTERVAL '8 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX043', 'C004', 'T014', NOW() - INTERVAL '7 days', 'sell', 60, 220.00, 13200.00, 'USD', 66.00, 12.00, 'KP043', NULL, 'pending'),
('TX044', 'C012', 'T001', '2023-01-15 10:00:00', 'buy', 80, 155.00, 12400.00, 'USD', 62.00, 10.00, 'KP044', '20230115 10:00:00', 'success'),
('TX045', 'C012', 'T001', '2023-01-16 10:30:00', 'buy', 80, 155.00, 12400.00, 'USD', 62.00, 10.00, 'KP045', '20230116 10:30:00', 'success'),
('TX046', 'C013', 'T002', '2023-02-20 11:30:00', 'sell', 60, 285.00, 17100.00, 'USD', 85.50, 15.00, 'KP046', '20230220 11:30:00', 'success'),
('TX047', 'C014', 'T003', '2023-03-25 14:45:00', 'buy', 5, 3250.00, 16250.00, 'USD', 81.25, 20.00, 'KP047', '20230325 14:45:00', 'success'),
('TX048', 'C012', 'T004', '2023-01-30 13:15:00', 'sell', 40, 190.00, 7600.00, 'USD', 38.00, 10.00, 'KP048', '20230130 13:15:00', 'success'),
('TX049', 'C013', 'T005', '2023-02-28 16:00:00', 'buy', 2, 2550.00, 5100.00, 'USD', 25.50, 15.00, 'KP049', '20230228 16:00:00', 'success'),
('TX050', 'C014', 'T006', '2023-03-30 09:45:00', 'sell', 30, 210.00, 6300.00, 'USD', 31.50, 10.00, 'KP050', '20230331 09:45:00', 'success'),
('TX051', 'C015', 'T001', DATE_TRUNC('month', NOW()) - INTERVAL '5 months' + INTERVAL '1 day', 'buy', 50, 150.00, 7500.00, 'USD', 37.50, 10.00, 'KP051', TO_CHAR(DATE_TRUNC('month', NOW()) - INTERVAL '5 months' + INTERVAL '1 day', '%Y%m%d %H:%i:%s'), 'success'),
('TX052', 'C016', 'T002', DATE_TRUNC('month', NOW()) - INTERVAL '4 months' + INTERVAL '2 days', 'sell', 40, 280.00, 11200.00, 'USD', 56.00, 10.00, 'KP052', TO_CHAR(DATE_TRUNC('month', NOW()) - INTERVAL '4 months' + INTERVAL '2 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX053', 'C017', 'T003', DATE_TRUNC('month', NOW()) - INTERVAL '3 months' + INTERVAL '3 days', 'buy', 15, 3200.00, 48000.00, 'USD', 240.00, 20.00, 'KP053', TO_CHAR(DATE_TRUNC('month', NOW()) - INTERVAL '3 months' + INTERVAL '3 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX054', 'C018', 'T004', DATE_TRUNC('month', NOW()) - INTERVAL '2 months' + INTERVAL '4 days', 'sell', 30, 180.00, 5400.00, 'USD', 27.00, 5.00, 'KP054', TO_CHAR(DATE_TRUNC('month', NOW()) - INTERVAL '2 months' + INTERVAL '4 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX055', 'C019', 'T005', DATE_TRUNC('month', NOW()) - INTERVAL '1 month' + INTERVAL '5 days', 'buy', 10, 2500.00, 25000.00, 'USD', 125.00, 15.00, 'KP055', TO_CHAR(DATE_TRUNC('month', NOW()) - INTERVAL '1 month' + INTERVAL '5 days', '%Y%m%d %H:%i:%s'), 'success'),
('TX056', 'C002', 'T006', DATE_TRUNC('month', NOW()) + INTERVAL '1 day', 'sell', 20, 200.00, 4000.00, 'USD', 20.00, 10.00, 'KP056', TO_CHAR(DATE_TRUNC('month', NOW()) + INTERVAL '1 day', '%Y%m%d %H:%i:%s'), 'success');

DROP TABLE IF EXISTS main.users CASCADE;
CREATE TABLE main.users (
  uid BIGINT PRIMARY KEY,
  username VARCHAR(50) NOT NULL,
  email VARCHAR(100) NOT NULL,
  phone_number VARCHAR(20),
  created_at TIMESTAMP NOT NULL,
  last_login_at TIMESTAMP,
  user_type VARCHAR(20) NOT NULL, -- possible values: individual, business, admin
  status VARCHAR(20) NOT NULL, -- possible values: active, inactive, suspended, deleted
  country VARCHAR(2), -- 2-letter country code
  address_billing TEXT,
  address_delivery TEXT,
  kyc_status VARCHAR(20), -- possible values: pending, approved, rejected
  kyc_verified_at TIMESTAMP
);

DROP TABLE IF EXISTS main.merchants CASCADE;
CREATE TABLE main.merchants (
  mid BIGINT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  description TEXT,
  website_url VARCHAR(200),
  logo_url VARCHAR(200),
  created_at TIMESTAMP NOT NULL,
  country VARCHAR(2), -- 2-letter country code
  state VARCHAR(50),
  city VARCHAR(50),
  postal_code VARCHAR(20),
  address TEXT,
  status VARCHAR(20) NOT NULL, -- possible values: active, inactive, suspended
  category VARCHAR(50),
  sub_category VARCHAR(50),
  mcc INT, -- Merchant Category Code
  contact_name VARCHAR(100),
  contact_email VARCHAR(100),
  contact_phone VARCHAR(20)
);

DROP TABLE IF EXISTS main.coupons CASCADE;
CREATE TABLE main.coupons (
  cid BIGINT PRIMARY KEY,
  merchant_id BIGINT NOT NULL REFERENCES main.merchants(mid),
  code VARCHAR(20) NOT NULL,
  description TEXT,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  discount_type VARCHAR(20) NOT NULL, -- possible values: percentage, fixed_amount
  discount_value DECIMAL(10,2) NOT NULL,
  min_purchase_amount DECIMAL(10,2),
  max_discount_amount DECIMAL(10,2),
  redemption_limit INT,
  status VARCHAR(20) NOT NULL, -- possible values: active, inactive, expired
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP
);

-- Fact Tables --
DROP TABLE IF EXISTS main.wallet_transactions_daily CASCADE;
CREATE TABLE main.wallet_transactions_daily (
  txid SERIAL PRIMARY KEY,
  sender_id BIGINT NOT NULL,
  sender_type INT NOT NULL, -- 0 for user, 1 for merchant
  receiver_id BIGINT NOT NULL,
  receiver_type INT NOT NULL, -- 0 for user, 1 for merchant
  amount DECIMAL(10,2) NOT NULL,
  status VARCHAR(20) NOT NULL, -- possible values: pending, success, failed, refunded
  type VARCHAR(20) NOT NULL, -- possible values: credit, debit
  description TEXT,
  coupon_id BIGINT, -- NULL if transaction doesn't involve a coupon
  created_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP, -- NULL if failed
  transaction_ref VARCHAR(36) NOT NULL, -- randomly generated uuid4 for users' reference
  gateway_name VARCHAR(50),
  gateway_ref VARCHAR(50),
  device_id VARCHAR(50),
  ip_address VARCHAR(50),
  user_agent TEXT
);

DROP TABLE IF EXISTS main.wallet_user_balance_daily CASCADE;
CREATE TABLE main.wallet_user_balance_daily (
  user_id BIGINT,
  balance DECIMAL(10,2) NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS main.wallet_merchant_balance_daily CASCADE;
CREATE TABLE main.wallet_merchant_balance_daily (
  merchant_id BIGINT,
  balance DECIMAL(10,2) NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS main.notifications CASCADE;
CREATE TABLE main.notifications (
  id SERIAL PRIMARY KEY,
  user_id INT NOT NULL REFERENCES main.users(uid),
  message TEXT NOT NULL,
  type VARCHAR(50) NOT NULL, -- possible values: transaction, promotion, security, general 
  status VARCHAR(20) NOT NULL, -- possible values: unread, read, archived
  created_at TIMESTAMP NOT NULL,
  read_at TIMESTAMP, -- NULL if not read
  device_type VARCHAR(10), -- possible values: mobile_app, web_app, email, sms
  device_id VARCHAR(36),
  action_url TEXT -- can be external https or deeplink url within the app
);  

DROP TABLE IF EXISTS main.user_sessions CASCADE;
CREATE TABLE main.user_sessions (
  user_id BIGINT NOT NULL,
  session_start_ts TIMESTAMP NOT NULL,
  session_end_ts TIMESTAMP,
  device_type VARCHAR(10), -- possible values: mobile_app, web_app, email, sms
  device_id VARCHAR(36)
);

DROP TABLE IF EXISTS main.user_setting_snapshot CASCADE;
CREATE TABLE main.user_setting_snapshot (
  user_id BIGINT NOT NULL,
  snapshot_date DATE NOT NULL,
  tx_limit_daily DECIMAL(10,2),
  tx_limit_monthly DECIMAL(10,2),
  membership_status INTEGER, -- 0 for bronze, 1 for silver, 2 for gold, 3 for platinum, 4 for VIP
  password_hash VARCHAR(255),
  api_key VARCHAR(255),
  verified_devices TEXT, -- comma separated list of device ids
  verified_ips TEXT, -- comma separated list of IP addresses
  mfa_enabled BOOLEAN,
  marketing_opt_in BOOLEAN,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id, snapshot_date)
);

-- users
INSERT INTO main.users (uid, username, email, phone_number, created_at, user_type, status, country, address_billing, address_delivery, kyc_status) 
VALUES 
  (1, 'john_doe', 'john.doe@email.com', '+1234567890', DATE_TRUNC('month', CURRENT_TIMESTAMP) - INTERVAL '1 month', 'individual', 'active', 'US', '123 Main St, Anytown US 12345', '123 Main St, Anytown US 12345', 'approved'),
  (2, 'jane_smith', 'jane.smith@email.com', '+9876543210', DATE_TRUNC('month', CURRENT_TIMESTAMP) - INTERVAL '2 months', 'individual', 'active', 'CA', '456 Oak Rd, Toronto ON M1M2M2', '456 Oak Rd, Toronto ON M1M2M2', 'approved'), 
  (3, 'bizuser', 'contact@business.co', '+1234509876', '2021-06-01 09:15:00', 'business', 'active', 'FR', '12 Rue Baptiste, Paris 75001', NULL, 'approved'),
  (4, 'david_miller', 'dave@personal.email', '+4477788899', '2023-03-20 18:45:00', 'individual', 'inactive', 'GB', '25 London Road, Manchester M12 4XY', '25 London Road, Manchester M12 4XY', 'pending'),
  (5, 'emily_wilson', 'emily.w@gmail.com', '+8091017161', '2021-11-03 22:10:00', 'individual', 'suspended', 'AU', '72 Collins St, Melbourne VIC 3000', '19 Smith St, Brunswick VIC 3056', 'rejected'),
  (6, 'techcorp', 'orders@techcorp.com', '+14165558888', '2018-05-20 11:35:00', 'business', 'active', 'US', '33 Technology Dr, Silicon Valley CA 94301', NULL, 'approved'),
  (7, 'shopsmart', 'customerserv@shopsmart.biz', '+6585771234', '2020-09-15 06:25:00', 'business', 'inactive', 'SG', '888 Orchard Rd, #05-000, Singapore 238801', NULL, 'approved'),
  (8, 'michael_brown', 'mike.brown@outlook.com', '+3912378624', '2019-07-22 16:40:00', 'individual', 'active', 'DE', 'Heidestr 17, Berlin 10557', 'Heidestr 17, Berlin 10557', 'approved'),
  (9, 'alex_taylor', 'ataylo@university.edu', NULL, '2022-08-30 09:15:00', 'individual', 'active', 'NZ', '12 Mardon Rd, Wellington 6012', '5 Boulcott St, Wellington 6011', 'approved'),
  (10, 'huang2143', 'huang2143@example.com', '+8612345678901', '2023-12-10 08:00:00', 'individual', 'active', 'CN', '123 Nanjing Road, Shanghai 200000', '123 Nanjing Road, Shanghai 200000', 'approved'),
  (11, 'lisa_jones', 'lisa.jones@email.com', '+6123456789', '2023-09-05 15:20:00', 'individual', 'active', 'AU', '789 George St, Sydney NSW 2000', '789 George St, Sydney NSW 2000', 'approved');

-- merchants 
INSERT INTO main.merchants (mid, name, description, website_url, logo_url, created_at, country, state, city, postal_code, address, status, category, sub_category, mcc, contact_name, contact_email, contact_phone)
VALUES
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

-- coupons
INSERT INTO main.coupons (cid, merchant_id, code, description, start_date, end_date, discount_type, discount_value, min_purchase_amount, max_discount_amount, redemption_limit, status, created_at, updated_at)
VALUES
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


-- wallet_transactions_daily
INSERT INTO main.wallet_transactions_daily (txid, sender_id, sender_type, receiver_id, receiver_type, amount, status, type, description, coupon_id, created_at, completed_at, transaction_ref, gateway_name, gateway_ref, device_id, ip_address, user_agent)
VALUES
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
  (21, 1, 0, 2, 1, 49.99, 'success', 'debit', 'Product purchase', NULL, NOW() - INTERVAL '5 months', NOW() - INTERVAL '5 months', 'tx_ref_11_1', 'Stripe', 'stripe_ref_11_1', 'device_11_1', '192.168.1.11', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36'),
  (22, 4, 0, 3, 1, 99.99, 'success', 'debit', 'Service purchase', NULL, NOW() - INTERVAL '4 months', NOW() - INTERVAL '4 months', 'tx_ref_12_1', 'PayPal', 'paypal_ref_12_1', 'device_12_1', '192.168.1.12', 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1'),
  (23, 4, 0, 1, 1, 149.99, 'success', 'debit', 'Subscription purchase', NULL, NOW() - INTERVAL '3 months', NOW() - INTERVAL '3 months', 'tx_ref_13_1', 'Stripe', 'stripe_ref_13_1', 'device_13_1', '192.168.1.13', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36'),
  (24, 2, 0, 5, 1, 199.99, 'pending', 'debit', 'Product purchase', NULL, NOW() - INTERVAL '2 months', NOW() - INTERVAL '2 months', 'tx_ref_14_1', 'PayPal', 'paypal_ref_14_1', 'device_14_1', '192.168.1.14', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0'),
  (25, 2, 0, 1, 1, 249.99, 'success', 'debit', 'Service purchase', NULL, NOW() - INTERVAL '1 month', NOW() - INTERVAL '1 month', 'tx_ref_15_1', 'Stripe', 'stripe_ref_15_1', 'device_15_1', '192.168.1.15', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36'),
  (26, 7, 1, 2, 0, 299.99, 'success', 'debit', 'Renew subscription', NULL, NOW() - INTERVAL '3 weeks', NOW() - INTERVAL '3 weeks', 'ed6f46ab-9617-4d11-9aa9-55071127d9ad', 'PayPal', 'sub_pjk832', 'web_zld22f', '199.59.148.201', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ...');

  
-- wallet_user_balance_daily
INSERT INTO main.wallet_user_balance_daily (user_id, balance, updated_at)
VALUES
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
  (1, 2739.10, NOW() - INTERVAL '8 days'),
  (1, 2738.12, NOW() - INTERVAL '6 days'),
  (1, 2733.92, NOW() - INTERVAL '3 days'),
  (2, 155.24, NOW() - INTERVAL '7 days'),
  (3, 2775.25, NOW() - INTERVAL '6 days'),
  (4, 2500.00, NOW() - INTERVAL '5 days'),
  (5, 155.99, NOW() - INTERVAL '4 days'),
  (6, 29.95, NOW() - INTERVAL '3 days'),
  (7, 172.98, NOW() - INTERVAL '2 days'),
  (8, 0.00, NOW() - INTERVAL '7 days'),
  (9, 125.00, NOW() - INTERVAL '3 days'),
  (10, 219.98, NOW() - INTERVAL '1 days');
  
-- wallet_merchant_balance_daily  
INSERT INTO main.wallet_merchant_balance_daily (merchant_id, balance, updated_at)
VALUES
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
  (1, 82.10, CURRENT_DATE - INTERVAL '8 days'),
  (2, 82.12, CURRENT_DATE - INTERVAL '8 days'),
  (1, 82.92, CURRENT_DATE - INTERVAL '7 days'),
  (2, 55.24, CURRENT_DATE - INTERVAL '7 days'),
  (3, 75.25, CURRENT_DATE - INTERVAL '7 days'),
  (1, 50.00, CURRENT_DATE),
  (2, 55.99, CURRENT_DATE),
  (3, 29.95, CURRENT_DATE),
  (4, 89.99, CURRENT_DATE),
  (5, 599.99, CURRENT_DATE);
  
-- notifications
INSERT INTO main.notifications (id, user_id, message, type, status, created_at, device_type, device_id, action_url)
VALUES
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
(13, 1, 'New promotion: Get 10% off your next order!', 'promotion', 'unread', DATE_TRUNC('week', CURRENT_TIMESTAMP) - INTERVAL '1 week', 'email', NULL, 'https://techmart.com/promo/TECH10'),
(14, 1, 'Your order #456def has been delivered', 'transaction', 'unread', DATE_TRUNC('week', CURRENT_TIMESTAMP) - INTERVAL '2 weeks', 'mobile_app', 'mobile_8fh2k1', 'app://orders/456def'),  
(15, 2, 'Reminder: Your FitLife membership expires in 7 days', 'general', 'unread', DATE_TRUNC('week', CURRENT_TIMESTAMP) - INTERVAL '3 weeks', 'email', NULL, 'https://fitlifegear.com/renew'),
(16, 2, 'Weekend Flash Sale: 25% off all activewear!', 'promotion', 'unread', DATE_TRUNC('week', CURRENT_TIMESTAMP) - INTERVAL '1 week' + INTERVAL '2 days', 'mobile_app', 'mobile_yjp08q', 'app://shop/activewear');

-- user_sessions
INSERT INTO main.user_sessions (user_id, session_start_ts, session_end_ts, device_type, device_id)
VALUES
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
(5, CURRENT_TIMESTAMP - INTERVAL '32 days', CURRENT_TIMESTAMP - INTERVAL '32 days' + INTERVAL '15 min', 'web_app', 'web_8902wknz'),
(6, CURRENT_TIMESTAMP - INTERVAL '8 days', CURRENT_TIMESTAMP - INTERVAL '8 days' + INTERVAL '15 min', 'web_app', 'web_zz91p44l'),
(7, CURRENT_TIMESTAMP - INTERVAL '5 days', CURRENT_TIMESTAMP - INTERVAL '5 days' + INTERVAL '15 min', 'web_app', 'web_zz91p44l'),
(8, CURRENT_TIMESTAMP - INTERVAL '3 days', CURRENT_TIMESTAMP - INTERVAL '3 days' + INTERVAL '15 min', 'web_app', 'web_d8180kaf'),
(9, CURRENT_TIMESTAMP - INTERVAL '1 days', CURRENT_TIMESTAMP - INTERVAL '1 days' + INTERVAL '15 min', 'mobile_app', 'mobile_g3mjfz'),
(10, CURRENT_TIMESTAMP - INTERVAL '2 days', CURRENT_TIMESTAMP - INTERVAL '2 days' + INTERVAL '15 min', 'web_app', 'web_zz91p44l'),
(5, CURRENT_TIMESTAMP - INTERVAL '2 days', CURRENT_TIMESTAMP - INTERVAL '2 days' + INTERVAL '15 min', 'web_app', 'web_8902wknz')
;

-- user_setting_snapshot
INSERT INTO main.user_setting_snapshot (user_id, snapshot_date, tx_limit_daily, tx_limit_monthly, membership_status, password_hash, api_key, verified_devices, verified_ips, mfa_enabled, marketing_opt_in, created_at)
VALUES
(1, '2023-06-07', 1000.00, 5000.00, 2, 'bcryptHash($2yz9!&ka1)', '9d61c49b-8977-4914-a36b-80d1445e38fa', 'mobile_8fh2k1', '192.168.0.1', true, false, '2023-06-07 00:00:00'),
(2, '2023-06-07', 500.00, 2500.00, 1, 'bcryptHash(qpwo9874zyGk!)', NULL, 'mobile_yjp08q, mobile_1av8p0', '198.51.100.233, 70.121.39.25', false, true, '2023-06-07 00:00:00'),
(3, '2023-06-07', 2000.00, 10000.00, 3, 'bcryptHash(Fr3nchPa1n!@98zy)', 'e785f611-fdd8-4c2d-a870-e104358712e5', 'web_k29qjd, mobile_x28qlj', '216.58.195.68, 92.110.51.150', true, false, '2023-06-07 00:00:00'),
(4, '2023-06-07', 5000.00, 20000.00, 4, 'bcryptHash(Vacay2023*&!Rm)', NULL, 'mobile_34jdkl', '143.92.64.138', false, true, '2023-06-07 00:00:00'),
(5, '2023-06-07', 100.00, 500.00, 0, 'bcryptHash(cRaf7yCr8zy)', NULL, 'web_8902wknz', '192.64.112.188', false, false, '2023-06-07 00:00:00'),
(6, '2023-06-07', 50.00, 500.00, 1, 'bcryptHash(C0d3Rul3z!99)', '6c03c175-9ac9-4854-b064-a3fff2c62e31', 'web_zz91p44l', '4.14.15.90', true, true, '2023-06-07 00:00:00'),
(7, '2023-06-07', 250.00, 1000.00, 2, 'bcryptHash(zEnH0me&Pw7)', NULL, NULL, NULL, false, true, '2023-06-07 00:00:00'),
(8, '2023-06-07', 200.00, 1000.00, 0, 'bcryptHash(K1dzPlay!&Rt8)', NULL, 'web_d8180kaf, mobile_q3mz8n', '8.26.53.165, 68.85.32.201', false, false, '2023-06-07 00:00:00'),
(9, '2023-06-07', 150.00, 1000.00, 2, 'bcryptHash(Gl0wUp7!9zy)', NULL, 'mobile_g3mjfz', '203.96.81.36', true, true, '2023-06-07 00:00:00'),
(10, '2023-06-07', 300.00, 2000.00, 1, 'bcryptHash(GamzRu1ez*&99!)', NULL, 'web_d8180kaf', '8.26.53.165', false, true, '2023-06-07 00:00:00'),
(1, '2023-06-01', 502.00, 1000.00, 2, 'bcryptHash($2yz9!&ka1)', '9d61c49b-8977-4914-a36b-80d1445e38fa', 'mobile_8fh2k1', '192.168.0.1', false, true, '2023-06-01 06:00:00'),
(2, '2023-06-01', 500.00, 2500.00, 1, 'bcryptHash(qpwo9874zyGk!)', NULL, 'mobile_yjp08q', '198.51.100.233, 70.121.39.25', true, false, '2023-06-01 09:00:00');

DROP TABLE IF EXISTS main.cars CASCADE;
CREATE TABLE main.cars (
  _id SERIAL PRIMARY KEY,
  make TEXT NOT NULL, -- manufacturer of the car
  model TEXT NOT NULL, -- model name of the car
  year INTEGER NOT NULL, -- year of manufacture
  color TEXT NOT NULL, -- color of the car
  vin_number VARCHAR(17) NOT NULL UNIQUE, -- Vehicle Identification Number
  engine_type TEXT NOT NULL, -- type of engine (e.g., V6, V8, Electric)
  transmission TEXT NOT NULL, -- type of transmission (e.g., Automatic, Manual)
  cost NUMERIC(10, 2) NOT NULL, -- cost of the car
  crtd_ts TIMESTAMP NOT NULL DEFAULT NOW() -- timestamp when the car was added to the system
);

DROP TABLE IF EXISTS main.salespersons CASCADE;
CREATE TABLE main.salespersons (
  _id SERIAL PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE,
  phone VARCHAR(20) NOT NULL,
  hire_date DATE NOT NULL,
  termination_date DATE,
  crtd_ts TIMESTAMP NOT NULL DEFAULT NOW()
);

DROP TABLE IF EXISTS main.customers CASCADE;
CREATE TABLE main.customers (
  _id SERIAL PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE,
  phone VARCHAR(20) NOT NULL,
  address TEXT NOT NULL,
  city TEXT NOT NULL,
  state TEXT NOT NULL,
  zip_code VARCHAR(10) NOT NULL,
  crtd_ts TIMESTAMP NOT NULL DEFAULT NOW()
);

DROP TABLE IF EXISTS main.sales CASCADE;
CREATE TABLE main.sales (
  _id SERIAL PRIMARY KEY,
  car_id INTEGER NOT NULL REFERENCES main.cars(_id),
  salesperson_id INTEGER NOT NULL REFERENCES main.salespersons(_id),
  customer_id INTEGER NOT NULL REFERENCES main.customers(_id),
  sale_price NUMERIC(10, 2) NOT NULL,
  sale_date DATE NOT NULL,
  crtd_ts TIMESTAMP NOT NULL DEFAULT NOW()
);

DROP TABLE IF EXISTS main.inventory_snapshots CASCADE;
CREATE TABLE main.inventory_snapshots (
  id SERIAL PRIMARY KEY,
  snapshot_date DATE NOT NULL,
  car_id INTEGER NOT NULL REFERENCES main.cars(_id),
  is_in_inventory BOOLEAN NOT NULL,
  crtd_ts TIMESTAMP NOT NULL DEFAULT NOW()
);

DROP TABLE IF EXISTS main.payments_received CASCADE;
CREATE TABLE main.payments_received (
  id SERIAL PRIMARY KEY,
  sale_id INTEGER NOT NULL REFERENCES main.sales(_id),
  payment_date DATE NOT NULL,
  payment_amount NUMERIC(10, 2) NOT NULL,
  payment_method TEXT NOT NULL, -- values: cash, check, credit_card, debit_card, financing
  crtd_ts TIMESTAMP NOT NULL DEFAULT NOW()
);

DROP TABLE IF EXISTS main.payments_made CASCADE;
CREATE TABLE main.payments_made (
  id SERIAL PRIMARY KEY,
  vendor_name TEXT NOT NULL,
  payment_date DATE NOT NULL,
  payment_amount NUMERIC(10, 2) NOT NULL,
  payment_method TEXT NOT NULL, -- values: check, bank_transfer, credit_card
  invoice_number VARCHAR(50) NOT NULL,
  invoice_date DATE NOT NULL,
  due_date DATE NOT NULL,
  crtd_ts TIMESTAMP NOT NULL DEFAULT NOW()
);


-- cars
INSERT INTO main.cars (_id, make, model, year, color, vin_number, engine_type, transmission, cost)
VALUES
  (1, 'Toyota', 'Camry', 2022, 'Silver', '4T1BF1FK3CU510984', 'V6', 'Automatic', 28500.00),
  (2, 'Honda', 'Civic', 2021, 'platinum/grey', '2HGFC2F53MH522780', 'Inline 4', 'CVT', 22000.00),
  (3, 'Ford', 'Mustang', 2023, 'blue', '1FA6P8TH4M5100001', 'V8', 'Manual', 45000.00),
  (4, 'Tesla', 'Model 3', 2022, 'fuschia', '5YJ3E1EB7MF123456', 'Electric', 'Automatic', 41000.00),
  (5, 'Chevrolet', 'Equinox', 2021, 'midnight blue', '2GNAXUEV1M6290124', 'Inline 4', 'Automatic', 26500.00),
  (6, 'Nissan', 'Altima', 2022, 'Jet black', '1N4BL4BV4NN123456', 'V6', 'CVT', 25000.00),
  (7, 'BMW', 'X5', 2023, 'Titan Silver', '5UXCR6C56M9A12345', 'V8', 'Automatic', 62000.00),
  (8, 'Audi', 'A4', 2022, 'Blue', 'WAUBNAF47MA098765', 'Inline 4', 'Automatic', 39000.00),
  (9, 'Lexus', 'RX350', 2021, 'Fiery red', '2T2BZMCA7MC143210', 'V6', 'Automatic', 45500.00),
  (10, 'Subaru', 'Outback', 2022, 'Jade', '4S4BSANC2N3246801', 'Boxer 4', 'CVT', 28000.00),
  (11, 'Mazda', 'CX-5', 2022, 'Royal Purple', 'JM3KE4DY4N0123456', 'Inline 4', 'Automatic', 29000.00),
  (12, 'Hyundai', 'Tucson', 2023, 'black', 'KM8J3CAL3NU123456', 'Inline 4', 'Automatic', 32000.00),
  (13, 'Kia', 'Sorento', 2021, 'ebony black', '5XYPH4A50MG987654', 'V6', 'Automatic', 32000.00),
  (14, 'Jeep', 'Wrangler', 2022, 'Harbor Gray', '1C4HJXDG3NW123456', 'V6', 'Automatic', 38000.00),
  (15, 'GMC', 'Sierra 1500', 2023, 'Snow White', '1GTU9CED3NZ123456', 'V8', 'Automatic', 45000.00),
  (16, 'Ram', '1500', 2022, 'baby blue', '1C6SRFFT3NN123456', 'V8', 'Automatic', 42000.00),
  (17, 'Mercedes-Benz', 'E-Class', 2021, 'Silver', 'W1KZF8DB1MA123456', 'Inline 6', 'Automatic', 62000.00),
  (18, 'Volkswagen', 'Tiguan', 2022, 'Red', '3VV2B7AX1NM123456', 'Inline 4', 'Automatic', 32000.00),
  (19, 'Volvo', 'XC90', 2023, 'black', 'YV4A22PK3N1234567', 'Inline 4', 'Automatic', 65000.00),
  (20, 'Porsche', '911', 2022, 'white', 'WP0AA2A93NS123456', 'Flat 6', 'Automatic', 120000.00),
  (21, 'Cadillac', 'Escalade', 2023, 'Black', '1GYS4HKJ3MR123456', 'V8', 'Automatic', 85000.00);

-- salespersons
INSERT INTO main.salespersons (_id, first_name, last_name, email, phone, hire_date, termination_date)
VALUES
  (1, 'John', 'Doe', 'john.doe@autonation.com', '(555)-123-4567', CURRENT_DATE - INTERVAL '2 years', NULL),
  (2, 'Jane', 'Smith', 'jane.smith@autonation.com', '(415)-987-6543', CURRENT_DATE - INTERVAL '3 years', NULL),
  (3, 'Michael', 'Johnson', 'michael.johnson@autonation.com', '(555)-456-7890', CURRENT_DATE - INTERVAL '1 year', NULL),
  (4, 'Emily', 'Brown', 'emily.brown@sonicauto.com', '(444)-111-2222', CURRENT_DATE - INTERVAL '1 year', CURRENT_DATE - INTERVAL '1 month'),
  (5, 'David', 'Wilson', 'david.wilson@sonicauto.com', '(444)-333-4444', CURRENT_DATE - INTERVAL '2 years', NULL),
  (6, 'Sarah', 'Taylor', 'sarah.taylor@sonicauto.com', '(123)-555-6666', '2018-09-01', '2022-09-01'),
  (7, 'Daniel', 'Anderson', 'daniel.anderson@sonicauto.com', '(555)-777-8888', '2021-07-12', NULL),
  (8, 'Olivia', 'Thomas', 'olivia.thomas@pensake.com', '(333)-415-0000', '2023-01-25', '2023-07-25'),
  (9, 'James', 'Jackson', 'james.jackson@pensake.com', '(555)-212-3333', '2019-04-30', NULL),
  (10, 'Sophia', 'White', 'sophia.white@pensake.com', '(555)-444-5555', '2022-08-18', NULL),
  (11, 'Robert', 'Johnson', 'robert.johnson@pensake.com', '(001)-415-5678', CURRENT_DATE - INTERVAL '15 days', NULL),
  (12, 'Jennifer', 'Davis', 'jennifer.davis@directauto.com', '(555)-345-6789', CURRENT_DATE - INTERVAL '20 days', NULL),
  (13, 'Jessica', 'Rodriguez', 'jessica.rodriguez@directauto.com', '(555)-789-0123', '2022-06-01', NULL);

-- customers
INSERT INTO main.customers (_id, first_name, last_name, email, phone, address, city, state, zip_code, crtd_ts)
VALUES
  (1, 'William', 'Davis', 'william.davis@example.com', '555-888-9999', '123 Main St', 'New York', 'NY', '10001', NOW() - INTERVAL '5 years'),
  (2, 'Ava', 'Miller', 'ava.miller@example.com', '555-777-6666', '456 Oak Ave', 'Los Angeles', 'CA', '90001', NOW() - INTERVAL '4 years'),
  (3, 'Benjamin', 'Wilson', 'benjamin.wilson@example.com', '555-666-5555', '789 Elm St', 'Chicago', 'IL', '60007', NOW() - INTERVAL '3 years'),
  (4, 'Mia', 'Moore', 'mia.moore@example.com', '555-555-4444', '321 Pine Rd', 'Houston', 'TX', '77001', NOW() - INTERVAL '2 years'),
  (5, 'Henry', 'Taylor', 'henry.taylor@example.com', '555-444-3333', '654 Cedar Ln', 'Phoenix', 'AZ', '85001', NOW() - INTERVAL '1 year'),
  (6, 'Charlotte', 'Anderson', 'charlotte.anderson@example.com', '555-333-2222', '987 Birch Dr', 'Philadelphia', 'PA', '19019', NOW() - INTERVAL '5 years'),
  (7, 'Alexander', 'Thomas', 'alexander.thomas@example.com', '555-222-1111', '741 Walnut St', 'San Antonio', 'TX', '78006', NOW() - INTERVAL '4 years'),
  (8, 'Amelia', 'Jackson', 'amelia.jackson@gmail.com', '555-111-0000', '852 Maple Ave', 'San Diego', 'CA', '92101', NOW() - INTERVAL '3 years'),
  (9, 'Daniel', 'White', 'daniel.white@youtube.com', '555-000-9999', '963 Oak St', 'Dallas', 'TX', '75001', NOW() - INTERVAL '2 years'),
  (10, 'Abigail', 'Harris', 'abigail.harris@company.io', '555-999-8888', '159 Pine Ave', 'San Jose', 'CA', '95101', NOW() - INTERVAL '1 year'),
  (11, 'Christopher', 'Brown', 'christopher.brown@ai.com', '555-456-7890', '753 Maple Rd', 'Miami', 'FL', '33101', NOW() - INTERVAL '5 months'),
  (12, 'Sophia', 'Lee', 'sophia.lee@microsoft.com', '555-567-8901', '951 Oak Ln', 'Seattle', 'WA', '98101', NOW() - INTERVAL '6 months'),
  (13, 'Michael', 'Chen', 'michael.chen@company.com', '(555)-456-7890', '123 Oak St', 'San Francisco', 'CA', '94101', NOW() - INTERVAL '3 months');

-- sales
INSERT INTO main.sales (_id, car_id, salesperson_id, customer_id, sale_price, sale_date)
VALUES
  (1, 1, 2, 3, 30500.00, '2023-03-15'),
  (2, 3, 1, 5, 47000.00, '2023-03-20'),
  (3, 6, 4, 2, 26500.00, '2023-03-22'),
  (4, 8, 7, 9, 38000.00, '2023-03-25'),
  (5, 2, 4, 7, 23500.00, '2023-03-28'),
  (6, 10, 6, 1, 30000.00, '2023-04-01'),
  (7, 5, 3, 6, 26800.00, '2023-04-05'),
  (8, 7, 2, 10, 63000.00, '2023-04-10'),
  (9, 4, 6, 8, 42500.00, '2023-04-12'),
  (10, 9, 2, 4, 44500.00, '2023-04-15'),
  (11, 1, 7, 11, 28900.00, CURRENT_DATE - INTERVAL '32 days'),
  (12, 3, 3, 12, 46500.00, CURRENT_DATE - INTERVAL '10 days'),
  (13, 6, 1, 11, 26000.00, CURRENT_DATE - INTERVAL '15 days'),
  (14, 2, 3, 1, 23200.00, CURRENT_DATE - INTERVAL '21 days'),
  (15, 8, 6, 12, 43500.00, CURRENT_DATE - INTERVAL '3 days'),
  (16, 10, 4, 2, 29500.00, CURRENT_DATE - INTERVAL '5 days'),
  (17, 3, 2, 3, 46000.00, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week' + INTERVAL '1 day'),
  (18, 3, 2, 7, 47500.00, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week'),
  (19, 3, 2, 10, 46500.00, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week' - INTERVAL '1 day'),
  (20, 4, 1, 3, 48000.00, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '8 week' + INTERVAL '1 day'),
  (21, 4, 1, 7, 45000.00, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '8 week'),
  (22, 4, 1, 10, 49000.00, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '8 week' - INTERVAL '1 day');


-- inventory_snapshots
INSERT INTO main.inventory_snapshots (id, snapshot_date, car_id, is_in_inventory)
VALUES
  (1, '2023-03-15', 1, TRUE),
  (2, '2023-03-15', 2, TRUE),
  (3, '2023-03-15', 3, TRUE),
  (4, '2023-03-15', 4, TRUE),
  (5, '2023-03-15', 5, TRUE),
  (6, '2023-03-15', 6, TRUE),
  (7, '2023-03-15', 7, TRUE),
  (8, '2023-03-15', 8, TRUE),
  (9, '2023-03-15', 9, TRUE),
  (10, '2023-03-15', 10, TRUE),
  (11, '2023-03-20', 1, FALSE),
  (12, '2023-03-20', 3, FALSE),
  (13, '2023-03-22', 6, FALSE),
  (14, '2023-03-25', 8, FALSE),
  (15, '2023-03-28', 2, FALSE),
  (16, '2023-04-01', 10, FALSE),
  (17, '2023-04-05', 5, FALSE),
  (18, '2023-04-10', 7, FALSE),
  (19, '2023-04-12', 4, FALSE),
  (20, '2023-04-15', 9, FALSE),
  (21, '2023-03-28', 1, TRUE),
  (22, '2023-03-28', 3, TRUE),
  (23, '2023-03-28', 4, FALSE);

-- payments_received
INSERT INTO main.payments_received (id, sale_id, payment_date, payment_amount, payment_method)
VALUES
  (1, 1, '2023-03-15', 5000.00, 'check'),
  (2, 1, '2023-03-20', 22500.00, 'financing'),
  (3, 2, '2023-03-20', 44000.00, 'credit_card'),
  (4, 3, '2023-03-22', 24500.00, 'debit_card'),
  (5, 4, '2023-03-25', 38000.00, 'financing'),
  (6, 5, '2023-03-28', 21500.00, 'cash'),
  (7, 6, '2023-04-01', 27000.00, 'credit_card'),
  (8, 7, '2023-04-05', 26000.00, 'debit_card'),
  (9, 8, '2023-04-10', 60000.00, 'financing'),
  (10, 9, '2023-04-12', 40000.00, 'check'),
  (11, 10, '2023-04-15', 44500.00, 'credit_card'),
  (12, 11, CURRENT_DATE - INTERVAL '30 days', 28000.00, 'cash'),
  (13, 12, CURRENT_DATE - INTERVAL '3 days', 43500.00, 'credit_card'),
  (14, 13, CURRENT_DATE - INTERVAL '6 days', 24000.00, 'debit_card'),
  (15, 14, CURRENT_DATE - INTERVAL '1 days', 17200.00, 'financing'),
  (16, 15, CURRENT_DATE - INTERVAL '1 days', 37500.00, 'credit_card'),
  (17, 16, CURRENT_DATE - INTERVAL '5 days', 26500.00, 'debit_card'),
  (18, 17, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week' + INTERVAL '1 day', 115000.00, 'financing'),
  (19, 18, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week', 115000.00, 'credit_card'),
  (20, 19, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week' - INTERVAL '1 day', 115000.00, 'debit_card'),
  (21, 20, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '8 week' + INTERVAL '1 day', 115000.00, 'cash'),
  (22, 21, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '8 week', 115000.00, 'check'),
  (23, 22, DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '8 week' - INTERVAL '1 day', 115000.00, 'credit_card');

-- payments_made
INSERT INTO main.payments_made (id, vendor_name, payment_date, payment_amount, payment_method, invoice_number, invoice_date, due_date)
VALUES
  (1, 'Car Manufacturer Inc', '2023-03-01', 150000.00, 'bank_transfer', 'INV-001', '2023-02-25', '2023-03-25'),
  (2, 'Auto Parts Supplier', '2023-03-10', 25000.00, 'check', 'INV-002', '2023-03-05', '2023-04-04'),
  (3, 'Utility Company', '2023-03-15', 1500.00, 'bank_transfer', 'INV-003', '2023-03-01', '2023-03-31'),
  (4, 'Marketing Agency', '2023-03-20', 10000.00, 'credit_card', 'INV-004', '2023-03-15', '2023-04-14'),
  (5, 'Insurance Provider', '2023-03-25', 5000.00, 'bank_transfer', 'INV-005', '2023-03-20', '2023-04-19'),
  (6, 'Cleaning Service', '2023-03-31', 2000.00, 'check', 'INV-006', '2023-03-25', '2023-04-24'),
  (7, 'Car Manufacturer Inc', '2023-04-01', 200000.00, 'bank_transfer', 'INV-007', '2023-03-25', '2023-04-24'),
  (8, 'Auto Parts Supplier', '2023-04-10', 30000.00, 'check', 'INV-008', '2023-04-05', '2023-05-05'),
  (9, 'Utility Company', '2023-04-15', 1500.00, 'bank_transfer', 'INV-009', '2023-04-01', '2023-04-30'),
  (10, 'Marketing Agency', '2023-04-20', 15000.00, 'credit_card', 'INV-010', '2023-04-15', '2023-05-15'),
  (11, 'Insurance Provider', '2023-04-25', 5000.00, 'bank_transfer', 'INV-011', '2023-04-20', '2023-05-20'),
  (12, 'Cleaning Service', '2023-04-30', 2000.00, 'check', 'INV-012', '2023-04-25', '2023-05-25'),
  (13, 'Toyota Auto Parts', CURRENT_DATE - INTERVAL '5 days', 12500.00, 'bank_transfer', 'INV-013', CURRENT_DATE - INTERVAL '10 days', CURRENT_DATE + INTERVAL '20 days'),
  (14, 'Honda Manufacturing', CURRENT_DATE - INTERVAL '3 days', 18000.00, 'check', 'INV-014', CURRENT_DATE - INTERVAL '8 days', CURRENT_DATE + INTERVAL '22 days'),
  (15, 'Ford Supplier Co', CURRENT_DATE - INTERVAL '2 days', 22000.00, 'bank_transfer', 'INV-015', CURRENT_DATE - INTERVAL '7 days', CURRENT_DATE + INTERVAL '23 days'),
  (16, 'Tesla Parts Inc', CURRENT_DATE - INTERVAL '1 day', 15000.00, 'credit_card', 'INV-016', CURRENT_DATE - INTERVAL '6 days', CURRENT_DATE + INTERVAL '24 days'),
  (17, 'Chevrolet Auto', CURRENT_DATE, 20000.00, 'bank_transfer', 'INV-017', CURRENT_DATE - INTERVAL '5 days', CURRENT_DATE + INTERVAL '25 days');


-- doctor dimension table
DROP TABLE IF EXISTS main.doctors CASCADE;
CREATE TABLE main.doctors (
  doc_id SERIAL PRIMARY KEY,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  specialty TEXT, -- possible values: dermatology, immunology, general, oncology
  year_reg INT, -- year the doctor was registered and obtained license
  med_school_name VARCHAR(100),
  loc_city VARCHAR(50),
  loc_state CHAR(2),
  loc_zip VARCHAR(10),
  bd_cert_num VARCHAR(20) -- board certification number
);

-- patient dimension table
DROP TABLE IF EXISTS main.patients CASCADE;
CREATE TABLE main.patients (
  patient_id SERIAL PRIMARY KEY,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  date_of_birth DATE,
  date_of_registration DATE,
  gender VARCHAR(10), -- Male, Female, Others
  email VARCHAR(100),
  phone VARCHAR(20),
  addr_street VARCHAR(100),
  addr_city VARCHAR(50),
  addr_state CHAR(2), 
  addr_zip VARCHAR(10),
  ins_type TEXT, -- possible values: private, medicare, medicaid, uninsured
  ins_policy_num VARCHAR(20),
  height_cm FLOAT,
  weight_kg FLOAT
);

-- drug dimension table  
DROP TABLE IF EXISTS main.drugs CASCADE;
CREATE TABLE main.drugs (
  drug_id SERIAL PRIMARY KEY,
  drug_name VARCHAR(100),
  manufacturer VARCHAR(100),
  drug_type TEXT, -- possible values: biologic, small molecule, topical
  moa TEXT, -- mechanism of action
  fda_appr_dt DATE,  -- FDA approval date. NULL if drug is still under trial.
  admin_route TEXT, -- possible values: oral, injection, topical 
  dos_amt DECIMAL(10,2),
  dos_unit VARCHAR(20),
  dos_freq_hrs INT,
  ndc VARCHAR(20) -- National Drug Code  
);

-- diagnosis dimension table
DROP TABLE IF EXISTS main.diagnoses CASCADE;
CREATE TABLE main.diagnoses (
  diag_id SERIAL PRIMARY KEY,  
  diag_code VARCHAR(10),
  diag_name VARCHAR(100),
  diag_desc TEXT
);

-- treatment fact table
DROP TABLE IF EXISTS main.treatments CASCADE;
CREATE TABLE main.treatments (
  treatment_id SERIAL PRIMARY KEY,
  patient_id INT REFERENCES main.patients(patient_id),
  doc_id INT REFERENCES main.doctors(doc_id), 
  drug_id INT REFERENCES main.drugs(drug_id),
  diag_id INT REFERENCES main.diagnoses(diag_id),
  start_dt DATE,
  end_dt DATE,  -- NULL if treatment is ongoing
  is_placebo BOOLEAN,
  tot_drug_amt DECIMAL(10,2),
  drug_unit TEXT -- possible values: mg, ml, g
);

-- outcome fact table 
DROP TABLE IF EXISTS main.outcomes CASCADE;
CREATE TABLE main.outcomes (
  outcome_id SERIAL PRIMARY KEY,
  treatment_id INT REFERENCES main.treatments(treatment_id),
  assess_dt DATE,
  day7_lesion_cnt INT, -- lesion counts on day 7.
  day30_lesion_cnt INT,
  day100_lesion_cnt INT,
  day7_pasi_score DECIMAL(4,1), -- PASI score range 0-72
  day30_pasi_score DECIMAL(4,1),
  day100_pasi_score DECIMAL(4,1),
  day7_tewl DECIMAL(5,2), -- in g/m^2/h  
  day30_tewl DECIMAL(5,2),
  day100_tewl DECIMAL(5,2),  
  day7_itch_vas INT, -- visual analog scale 0-100
  day30_itch_vas INT,
  day100_itch_vas INT,
  day7_hfg DECIMAL(4,1), -- hair growth factor range 0-5  
  day30_hfg DECIMAL(4,1),
  day100_hfg DECIMAL(4,1)
);

DROP TABLE IF EXISTS main.adverse_events CASCADE;
CREATE TABLE main.adverse_events (
  id SERIAL PRIMARY KEY, -- 1 row per adverse event per treatment_id
  treatment_id INT REFERENCES main.treatments(treatment_id),
  reported_dt DATE,
  description TEXT
);

DROP TABLE IF EXISTS main.concomitant_meds CASCADE;
CREATE TABLE main.concomitant_meds (
  id SERIAL PRIMARY KEY, -- 1 row per med per treatment_id
  treatment_id INT REFERENCES main.treatments(treatment_id),
  med_name VARCHAR(100),
  start_dt TEXT, -- YYYY-MM-DD
  end_dt TEXT, -- YYYY-MM-DD NULL if still taking
  dose_amt DECIMAL(10,2),
  dose_unit TEXT, -- possible values: mg, ml, g
  freq_hrs INT
);

-- insert into dimension tables first

INSERT INTO main.doctors (doc_id, first_name, last_name, specialty, year_reg, med_school_name, loc_city, loc_state, loc_zip, bd_cert_num) 
VALUES
(1, 'John', 'Doe', 'dermatology', EXTRACT(YEAR FROM CURRENT_DATE) - 2, 'Johns Hopkins University', 'Baltimore', 'MD', '21201', 'ABC123'),
(2,'Jane', 'Smith', 'immunology', EXTRACT(YEAR FROM CURRENT_DATE) - 2, 'Harvard Medical School', 'Boston', 'MA', '02115', 'XYZ789'),
(3, 'David', 'Johnson', 'general', 1998, 'University of Pennsylvania', 'Philadelphia', 'PA', '19104', 'DEF456'),
(4, 'Emily', 'Brown', 'dermatology', 2015, 'Stanford University', 'Palo Alto', 'CA', '94304', 'GHI012'),
(5, 'Michael', 'Davis', 'immunology', 2008, 'Duke University', 'Durham', 'NC', '27708', 'JKL345'),
(6, 'Sarah', 'Wilson', 'oncology', EXTRACT(YEAR FROM CURRENT_DATE) - 1, 'University of California, San Francisco', 'San Francisco', 'CA', '94143', 'MNO678'),
(7, 'Robert', 'Taylor', 'dermatology', 2012, 'Yale University', 'New Haven', 'CT', '06510', 'PQR901'),
(8, 'Laura', 'Martinez', 'immunology', 2006, 'University of Michigan', 'Ann Arbor', 'MI', '48109', 'STU234'),
(9, 'Daniel', 'Garcia', 'general', EXTRACT(YEAR FROM CURRENT_DATE) - 3, 'University of Chicago', 'Chicago', 'IL', '60637', 'VWX567'),
(10, 'Olivia', 'Anderson', 'dermatology', 2018, 'Columbia University', 'New York', 'NY', '10027', 'YZA890');

INSERT INTO main.patients (patient_id, first_name, last_name, date_of_birth, date_of_registration, gender, email, phone, addr_street, addr_city, addr_state, addr_zip, ins_type, ins_policy_num, height_cm, weight_kg)
VALUES
(1, 'Alice', 'Johnson', '1985-03-15', '2023-01-03', 'Female', 'alice@email.com', '555-123-4567', '123 Main St', 'Anytown', 'CA', '12345', 'private', 'ABC123456', 165, 60),
(2, 'Bob', 'Smith', '1978-11-23', '2023-01-10', 'Male', 'bob@email.com', '555-987-6543', '456 Oak Ave', 'Somecity', 'NY', '54321', 'medicare', 'XYZ789012', 180, 85),
(3, 'Carol', 'Davis', '1992-07-08', '2022-01-03', 'Female', 'carol@email.com', '555-246-8135', '789 Elm Rd', 'Anothercity', 'TX', '67890', 'private', 'DEF345678', 158, 52),  
(4, 'David', 'Wilson', '1965-09-30', '2022-07-12', 'Male', 'david@email.com', '555-369-2580', '321 Pine Ln', 'Somewhere', 'FL', '13579', 'medicaid', 'GHI901234', 175, 78),
(5, 'Eve', 'Brown', '2000-01-01', '2023-08-03', 'Female', 'eve@email.com', '555-147-2589', '654 Cedar St', 'Nowhere', 'WA', '97531', 'uninsured', NULL, 160, 55),
(6, 'Frank', 'Taylor', '1988-05-12', '2021-12-21', 'Male', 'frank@email.com', '555-753-9514', '987 Birch Dr', 'Anyplace', 'CO', '24680', 'private', 'JKL567890', 183, 90),
(7, 'Grace', 'Anderson', '1975-12-25', '2023-09-04', 'Others', 'grace@email.com', '555-951-7532', '159 Maple Rd', 'Somewhere', 'OH', '86420', 'medicare', 'MNO246810', 170, 68),
(8, 'Hannah', 'Garcia', '1982-08-05', '2023-03-23', 'Female', 'hannah@email.com', '555-369-1470', '753 Walnut Ave', 'Somewhere', 'CA', '97531', 'private', 'PQR135790', 162, 57),
(9, 'Isaac', 'Martinez', '1995-02-18', '2021-11-13', 'Male', 'isaac@email.com', '555-147-8520', '951 Spruce Blvd', 'Anytown', 'TX', '13579', 'medicaid', 'STU024680', 178, 82),
(10, 'John', 'Richter', '1980-01-01', '2021-11-24', 'Male', 'john@qwik.com', '555-123-4567', '123 Main St', 'Anytown', 'CA', '12345', 'private', 'ABC123456', 180, 80),
(11, 'Kelly', 'Smith', '1985-05-15', '2024-02-28', 'Female', 'kelly@fsda.org', '555-987-6543', '456 Oak Ave', 'Somecity', 'NY', '54321', 'medicare', 'XYZ789012', 165, 60);



INSERT INTO main.drugs (drug_id, drug_name, manufacturer, drug_type, moa, fda_appr_dt, admin_route, dos_amt, dos_unit, dos_freq_hrs, ndc)
VALUES  
(1, 'Drugalin', 'Pharma Inc', 'biologic', 'TNF-alpha inhibitor', '2010-01-15', 'injection', 40, 'mg', 336, '12345-678-90'),
(2, 'Medicol', 'Acme Pharma', 'small molecule', 'IL-17A inhibitor', '2015-06-30', 'oral', 30, 'mg', 24, '54321-012-34'),
(3, 'Topizol', 'BioMed Ltd', 'topical', 'PDE4 inhibitor', '2018-11-01', 'topical', 15, 'g', 12, '98765-432-10'),
(4, 'Biologic-X', 'Innova Biologics', 'biologic', 'IL-23 inhibitor', NULL, 'injection', 100, 'mg', 672, '13579-246-80'), 
(5, 'Smallazine', 'Chem Co', 'small molecule', 'JAK inhibitor', '2020-03-15', 'oral', 5, 'mg', 24, '97531-864-20'),
(6, 'Topicort', 'Derma Rx', 'topical', 'Corticosteroid', '2005-09-30', 'topical', 30, 'g', 12, '24680-135-79'),
(7, 'Biologic-Y', 'BioPharm Inc', 'biologic', 'IL-12/23 inhibitor', '2012-07-01', 'injection', 50, 'mg', 504, '75319-951-46'),
(8, 'Smallitol', 'PharmaGen', 'small molecule', 'IL-6 inhibitor', '2017-04-15', 'oral', 10, 'mg', 24, '36915-258-07'),
(9, 'Topicalin', 'DermiCare', 'topical', 'Calcineurin inhibitor', '2019-10-01', 'topical', 20, 'g', 12, '14785-369-02'),
(10, 'Biologic-Z', 'BioMed Ltd', 'biologic', 'IL-17F inhibitor', '2021-01-01', 'injection', 80, 'mg', 336, '95146-753-19');

INSERT INTO main.diagnoses (diag_id, diag_code, diag_name, diag_desc)
VALUES
(1, 'L40.0', 'Psoriasis vulgaris', 'Plaque psoriasis, the most common form'),  
(2, 'L40.1', 'Generalized pustular psoriasis', 'Widespread pustules on top of red skin'),
(3, 'L40.4', 'Guttate psoriasis', 'Small, teardrop-shaped lesions'), 
(4, 'L40.8', 'Other psoriasis', 'Includes flexural, erythrodermic, and other rare types'),
(5, 'L40.9', 'Psoriasis, unspecified', 'Psoriasis not further specified'),
(6, 'L40.50', 'Arthropathic psoriasis, unspecified', 'Psoriatic arthritis, unspecified'),
(7, 'L40.51', 'Distal interphalangeal psoriatic arthropathy', 'Psoriatic arthritis mainly affecting the ends of fingers and toes'),
(8, 'L40.52', 'Psoriatic arthritis mutilans', 'Severe, deforming psoriatic arthritis'),   
(9, 'L40.53', 'Psoriatic spondylitis', 'Psoriatic arthritis of the spine'),
(10, 'L40.59', 'Other psoriatic arthropathy', 'Other specified types of psoriatic arthritis');

-- insert into fact tables
INSERT INTO main.treatments (treatment_id, patient_id, doc_id, drug_id, diag_id, start_dt, end_dt, is_placebo, tot_drug_amt, drug_unit)
VALUES
(1, 1, 1, 1, 1, '2022-01-01', '2022-06-30', false, 240, 'mg'),
(2, 2, 2, 2, 2, '2022-02-15', '2022-08-14', true, 180, 'mg'),
(3, 3, 3, 3, 3, '2022-03-10', '2022-09-09', false, 360, 'g'),
(4, 4, 4, 4, 4, '2022-04-01', NULL, false, 200, 'mg'),
(5, 5, 5, 5, 5, '2022-05-01', '2022-10-31', false, 180, 'mg'),
(6, 6, 6, 6, 6, '2022-06-15', '2022-12-14', false, 720, 'g'),
(7, 1, 7, 1, 7, '2022-07-01', '2022-12-31', true, 240, 'mg'),
(8, 2, 1, 2, 8, '2022-08-01', '2023-01-31', false, 180, 'mg'),
(9, 3, 2, 3, 9, '2022-09-01', '2023-02-28', false, 360, 'g'),
(10, 4, 3, 4, 10, '2022-10-01', NULL, true, 0, NULL),
(11, 5, 4, 5, 1, '2022-11-01', '2023-04-30', true, 180, 'mg'),
(12, 6, 5, 6, 2, '2022-12-01', '2023-05-31', false, 720, 'g'),
(13, 7, 6, 1, 3, '2023-01-01', '2023-06-30', false, 240, 'mg'),
(14, 1, 7, 2, 4, '2023-02-01', '2023-07-31', false, 180, 'mg'),
(15, 2, 1, 3, 5, '2023-03-01', '2023-08-31', false, 360, 'g'),
(16, 1, 2, 4, 6, DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2 year', DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2 months', false, 300, 'mg'),
(17, 2, 5, 1, 8, DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 year', DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '4 months', false, 80, 'mg'),
(18, 3, 6, 2, 9, DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months', NULL, true, 200, 'mg'),
(19, 1, 7, 3, 10, DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '4 months', NULL, false, 150, 'g'),
(20, 2, 1, 4, 1, DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 months', NULL, false, 100, 'mg'),
(21, 3, 2, 5, 2, DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2 months', NULL, false, 250, 'mg'),
(22, 1, 3, 6, 3, DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month', NULL, false, 300, 'g'),
(23, 2, 4, 1, 4, CURRENT_DATE, NULL, true, 200, 'mg'),
(24, 3, 5, 2, 5, CURRENT_DATE, NULL, false, 150, 'mg'),
(25, 9, 1, 1, 1, CURRENT_DATE - INTERVAL '6 months', CURRENT_DATE - INTERVAL '3 months', false, 240, 'mg'),
(26, 10, 2, 2, 2, CURRENT_DATE - INTERVAL '5 months', CURRENT_DATE - INTERVAL '2 months', false, 180, 'mg');

INSERT INTO main.outcomes (outcome_id, treatment_id, assess_dt, day7_lesion_cnt, day30_lesion_cnt, day100_lesion_cnt, day7_pasi_score, day30_pasi_score, day100_pasi_score, day7_tewl, day30_tewl, day100_tewl, day7_itch_vas, day30_itch_vas, day100_itch_vas, day7_hfg, day30_hfg, day100_hfg)  
VALUES
(1, 1, '2022-01-08', 20, 15, 5, 12.5, 8.2, 2.1, 18.2, 15.6, 12.1, 60, 40, 20, 1.5, 2.5, 4.0),
(2, 2, '2022-02-22', 25, 18, 8, 15.0, 10.1, 3.5, 20.1, 17.2, 13.5, 70, 50, 30, 1.0, 2.0, 3.5),
(3, 3, '2022-03-17', 18, 12, 3, 10.8, 6.4, 1.2, 16.5, 14.0, 10.8, 55, 35, 15, 2.0, 3.0, 4.5),
(4, 4, '2022-04-08', 30, 25, 12, 18.2, 13.9, 5.8, 22.4, 19.1, 15.2, 80, 60, 40, 0.5, 1.5, 3.0),
(5, 5, '2022-05-08', 22, 16, 6, 13.1, 8.7, 2.6, 19.0, 16.3, 12.7, 65, 45, 25, 1.2, 2.2, 3.8),
(6, 6, '2022-06-22', 28, 21, 10, 16.7, 11.5, 4.3, 21.3, 18.1, 14.3, 75, 55, 35, 0.8, 1.8, 3.3),
(7, 7, '2022-07-08', 19, 13, 4, 11.2, 6.9, 1.5, 17.1, 14.5, 11.2, 58, 38, 18, 1.8, 2.8, 4.3),
(8, 8, '2022-08-08', 26, 19, 9, 15.6, 10.6, 3.8, 20.7, 17.6, 13.9, 72, 52, 32, 0.7, 1.7, 3.2),
(9, 9, '2022-09-08', 21, 15, 5, 12.3, 8.0, 2.0, 18.6, 15.9, 12.4, 62, 42, 22, 1.4, 2.4, 3.9),
(10, 10, '2022-10-08', 32, 30, 25, 19.5, 17.8, 14.1, 23.2, 21.4, 18.7, 85, 80, 70, 0.2, 0.4, 0.8),
(11, 11, '2022-11-08', 23, 17, 7, 13.7, 9.2, 2.9, 19.5, 16.8, 13.1, 68, 48, 28, 1.1, 2.1, 3.6),
(12, 12, '2022-12-08', 29, 23, 11, 17.4, 12.3, 4.9, 21.8, 18.7, 14.8, 78, 58, 38, 0.6, 1.6, 3.1),
(13, 13, '2023-01-08', 18, 12, 3, 10.5, 6.1, 1.0, 16.9, 14.3, 11.0, 56, 36, 16, 1.9, 2.9, 4.4),
(14, 14, '2023-02-08', 27, 20, 10, 16.2, 11.1, 4.1, 21.0, 17.9, 14.1, 74, 54, 34, 0.5, 1.5, 3.0), 
(15, 15, '2023-03-08', 20, 14, 4, 11.8, 7.3, 1.7, 17.8, 15.2, 11.8, 60, 40, 20, 1.6, 2.6, 4.1),
(16, 16, DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '5 months' + INTERVAL '7 days', 24, 18, 8, 14.4, 9.6, 3.2, 20.4, 17.4, 13.7, 70, 50, 30, 0.9, 1.9, 3.4),
(17, 17, DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' + INTERVAL '7 days', 22, 16, NULL, 13.2, 8.8, NULL, 19.1, 16.3, NULL, 65, 45, NULL, 1.3, 2.3, NULL),
(18, 25, CURRENT_DATE - INTERVAL '6 months' + INTERVAL '7 days', 30, NULL, NULL, 18.0, NULL, NULL, 22.0, NULL, NULL, 80, NULL, NULL, 1.0, NULL, NULL),  
(19, 25, CURRENT_DATE - INTERVAL '2 months', 30, 18, 10, 18.0, 12.0, 4.0, 22.0, 19.0, 15.0, 80, 60, 40, 1.0, 2.0, 3.0),
(20, 26, CURRENT_DATE - INTERVAL '5 months' + INTERVAL '7 days', 25, NULL, NULL, 15.0, NULL, NULL, 20.0, NULL, NULL, 75, NULL, NULL, 0.5, NULL, NULL),
(21, 26, CURRENT_DATE - INTERVAL '1 month', 25, 18, 10, 15.0, 10.0, 5.0, 20.0, 17.0, 13.0, 75, 55, 35, 0.5, 1.5, 3.0);

INSERT INTO main.adverse_events (id, treatment_id, reported_dt, description)
VALUES  
(1, 1, '2022-01-15', 'Mild injection site reaction'),
(2, 2, '2022-02-28', 'Headache, nausea'),
(3, 4, '2022-04-10', 'Severe allergic reaction, hospitalization required'),
(4, 5, '2022-05-20', 'Upper respiratory infection'),
(5, 7, '2022-07-22', 'Mild injection site reaction'), 
(6, 9, '2022-09-18', 'Diarrhea'),
(7, 11, '2022-11-12', 'Elevated liver enzymes'),
(8, 14, '2023-02-05', 'Mild skin rash');

INSERT INTO main.concomitant_meds (id, treatment_id, med_name, start_dt, end_dt, dose_amt, dose_unit, freq_hrs)
VALUES
(1, 1, 'Acetaminophen', '2022-01-01', '2022-01-07', 500, 'mg', 6),
(2, 1, 'Ibuprofen', '2022-01-08', '2022-01-14', 200, 'mg', 8), 
(3, 2, 'Loratadine', '2022-02-15', '2022-03-15', 10, 'mg', 24),
(4, 3, 'Multivitamin', '2022-03-10', NULL, 1, 'tablet', 24),
(5, 4, 'Epinephrine', '2022-04-10', '2022-04-10', 0.3, 'mg', NULL),
(6, 4, 'Diphenhydramine', '2022-04-10', '2022-04-17', 50, 'mg', 6),
(7, 5, 'Amoxicillin', '2022-05-20', '2022-05-30', 500, 'mg', 8),
(8, 6, 'Calcium supplement', '2022-06-15', NULL, 600, 'mg', 24), 
(9, 7, 'Acetaminophen', '2022-07-15', '2022-07-21', 500, 'mg', 6),
(10, 8, 'Cetirizine', '2022-08-01', '2022-08-14', 10, 'mg', 24),
(11, 9, 'Loperamide', '2022-09-18', '2022-09-20', 4, 'mg', 6),
(12, 11, 'Ursodiol', '2022-11-30', '2022-12-30', 300, 'mg', 8),
(13, 12, 'Vitamin D', '2022-12-01', NULL, 1000, 'IU', 24),
(14, 13, 'Acetaminophen', '2023-01-08', '2023-01-14', 500, 'mg', 6),
(15, 14, 'Hydrocortisone cream', '2023-02-25', '2023-03-07', 10, 'g', 12);

CREATE SCHEMA IF NOT EXISTS keywords;

DROP TABLE IF EXISTS keywords."UPPERCASE_MASTER" CASCADE;
CREATE TABLE keywords."UPPERCASE_MASTER"(
  ID BIGINT NOT NULL PRIMARY KEY,
  NAME VARCHAR(30) NOT NULL,
  "CAST" VARCHAR(30),
  "WHERE" VARCHAR(30),
  "FROM" VARCHAR(30),
  "VARCHAR" VARCHAR(30),
  "INTEGER" VARCHAR(30),
  "TWO WORDS" VARCHAR(30),
  "ORDER BY" VARCHAR(30)
);

DROP TABLE IF EXISTS keywords.lowercase_detail CASCADE;
CREATE TABLE keywords.lowercase_detail (
  id BIGINT NOT NULL PRIMARY KEY,
  master_id BIGINT NOT NULL,
  "two words" VARCHAR(30),
  "select" VARCHAR(30),
  "as" VARCHAR(30),
  "0 = 0 and '" VARCHAR(30),
  result NUMERIC(10,2),
  is_active smallint,
  CONSTRAINT lowercase_to_UPPERCASE FOREIGN KEY (master_id) REFERENCES keywords."UPPERCASE_MASTER"(ID)
);

DROP TABLE IF EXISTS keywords."MixedCase_1:1" CASCADE;
CREATE TABLE keywords."MixedCase_1:1" (
  "Id" BIGINT NOT NULL PRIMARY KEY,
  "(parentheses)" VARCHAR(30),
  "In" smallint,
  "LowerCaseId" BIGINT NOT NULL,
  CONSTRAINT "MixedCase_1:1_to_UPPERCASE" FOREIGN KEY ("Id") REFERENCES keywords."UPPERCASE_MASTER"(ID),
  CONSTRAINT "MixedCase_1:1_to_lowercase_detail" FOREIGN KEY ("LowerCaseId") REFERENCES keywords.lowercase_detail(id)
);

DROP TABLE IF EXISTS keywords."CAST" CASCADE;
CREATE TABLE keywords."CAST" (
  PK_FIELD_NAME BIGINT NOT NULL PRIMARY KEY,
  ID BIGINT NOT NULL,
  ID2 BIGINT,
  is_active smallint,
  types VARCHAR(30),
  CONSTRAINT CAST_to_lowercase_detail1 FOREIGN KEY (ID) REFERENCES keywords.lowercase_detail(id),
  CONSTRAINT CAST_to_lowercase_detail2 FOREIGN KEY (ID2) REFERENCES keywords.lowercase_detail(id)
);

DROP TABLE IF EXISTS keywords."""QUOTED TABLE_NAME""" CASCADE;
CREATE TABLE keywords."""QUOTED TABLE_NAME""" (
  ID BIGINT NOT NULL PRIMARY KEY,
  "`cast`" BIGINT NOT NULL,
  "= ""QUOTE""" BIGINT NOT NULL,
  "`name""[" BIGINT NOT NULL,
  description VARCHAR(30),
  CONSTRAINT QUOTED_UNIQUE UNIQUE ("`cast`"),
  CONSTRAINT QUOTED_to_UPPERCASE_MASTER FOREIGN KEY ("`cast`") REFERENCES keywords."UPPERCASE_MASTER"(ID),
  CONSTRAINT QUOTED_to_lowercase_detail1 FOREIGN KEY ("= ""QUOTE""") REFERENCES keywords.lowercase_detail(id),
  CONSTRAINT QUOTED_to_lowercase_detail2 FOREIGN KEY ("`name""[") REFERENCES keywords.lowercase_detail(id)
);

DROP TABLE IF EXISTS keywords.CALCULATE CASCADE;
CREATE TABLE keywords.CALCULATE (
  ".WHERE" BIGINT NOT NULL PRIMARY KEY,
  "LOWER" INTEGER,
  "UPPER" INTEGER,
  "LENGTH" INTEGER,
  "STARTSWITH" INTEGER,
  "ENDSWITH" INTEGER,
  "CONTAINS" INTEGER,
  "LIKE" INTEGER,
  "JOIN_STRINGS" INTEGER,
  "LPAD" INTEGER,
  "RPAD" INTEGER,
  "FIND" INTEGER,
  "STRIP" INTEGER,
  "REPLACE" INTEGER,
  "STRCOUNT" INTEGER,
  "GETPART" INTEGER,
  DATETIME INTEGER,
  "YEAR" INTEGER,
  "QUARTER" INTEGER,
  "MONTH" INTEGER,
  "DAY" INTEGER,
  "HOUR" INTEGER,
  "MINUTE" INTEGER,
  "SECOND" INTEGER,
  "DATEDIFF" INTEGER,
  "DAYOFWEEK" INTEGER,
  "DAYNAME" INTEGER
);

DROP TABLE IF EXISTS keywords."WHERE" CASCADE;
CREATE TABLE keywords."WHERE" (
  ".CALCULATE" BIGINT NOT NULL PRIMARY KEY,
  "IFF" INTEGER,
  "ISIN" INTEGER,
  DEFAULT_TO INTEGER,
  PRESENT INTEGER,
  "ABSENT" INTEGER,
  "KEEP_IF" INTEGER,
  "MONOTONIC" INTEGER,
  ABS INTEGER,
  "ROUND" INTEGER,
  "CEIL" INTEGER,
  "FLOOR" INTEGER,
  "POWER" INTEGER,
  "SQRT" INTEGER,
  "SIGN" INTEGER,
  "SMALLEST" INTEGER,
  "LARGEST" INTEGER,
  "SUM" INTEGER,
  "AVG" INTEGER,
  "MEDIAN" INTEGER,
  "MIN" INTEGER,
  "MAX" INTEGER,
  "QUANTILE" INTEGER,
  "ANYTHING" INTEGER,
  "COUNT" INTEGER,
  "NDISTINCT" INTEGER,
  HAS INTEGER,
  "HASNOT" INTEGER,
  "VAR" INTEGER,
  "STD" INTEGER,
  CONSTRAINT WHERE_to_CALCULATE FOREIGN KEY (".CALCULATE") REFERENCES keywords.CALCULATE(".WHERE")
);

DROP TABLE IF EXISTS keywords."PARTITION" CASCADE;
CREATE TABLE keywords."PARTITION" (
  "ORDER_BY" BIGINT NOT NULL PRIMARY KEY,
  "CALCULATE" BIGINT NOT NULL,
  "WHERE" BIGINT NOT NULL,
  "TOP_K" INTEGER,
  "SINGULAR" INTEGER,
  "BEST" INTEGER,
  "CROSS" INTEGER,
  "RANKING" INTEGER,
  "PERCENTILE" INTEGER,
  "PREV" INTEGER,
  "NEXT" INTEGER,
  "RELSUM" INTEGER,
  "RELAVG" INTEGER,
  "RELCOUNT" INTEGER,
  "RELSIZE" INTEGER,
  "STRING" INTEGER,
  "INTEGER" INTEGER,
  "FLOAT" INTEGER,
  "NUMERIC" INTEGER,
  "DECIMAL" INTEGER,
  "CHAR" INTEGER,
  "VARCHAR" INTEGER,
  CONSTRAINT PARTITION_to_CALCULATE FOREIGN KEY ("CALCULATE") REFERENCES keywords.CALCULATE(".WHERE"),
  CONSTRAINT PARTITION_to_WHERE FOREIGN KEY ("WHERE") REFERENCES keywords."WHERE"(".CALCULATE")
);

DROP TABLE IF EXISTS keywords."COUNT" CASCADE;
CREATE TABLE keywords."COUNT" (
  "this" BIGINT NOT NULL,
  "class" BIGINT NOT NULL,
  "import" INTEGER,
  "def" INTEGER,
  "%%pydough" INTEGER,
  "node" INTEGER,
  """," INTEGER,
  "." INTEGER,
  "bool" INTEGER,
  "__call__" INTEGER,
  "int" INTEGER,
  "FLOAT" INTEGER,
  "__init__" INTEGER,
  "new" INTEGER,
  "del" INTEGER,
  "__col__" INTEGER,
  "__col1__" INTEGER,
  "__class__" INTEGER,
  "str" INTEGER,
  "dict" INTEGER,
  "__add__" INTEGER,
  "__mul__" INTEGER,
  CONSTRAINT "PK_COUNT" PRIMARY KEY ("this","class"),
  CONSTRAINT COUNT_to_CAST FOREIGN KEY ("this") REFERENCES keywords."CAST"(PK_FIELD_NAME)
);

DROP TABLE IF EXISTS keywords.master CASCADE;
CREATE TABLE keywords.master (
    id1 INT NOT NULL,
    id2 INT NOT NULL,
    alt_key1 INT NOT NULL,
    alt_key2 INT NOT NULL,
    description VARCHAR(30),
    CONSTRAINT PK_master PRIMARY KEY (id1, id2),
    CONSTRAINT AK_master_1 UNIQUE (alt_key1, alt_key2)
);

DROP TABLE IF EXISTS keywords.detail1 CASCADE;
CREATE TABLE keywords.detail1 (
    "key" INT,
    id1 INT NOT NULL,
    id2 INT NOT NULL,
    description VARCHAR(30),
    CONSTRAINT PK_detail1 PRIMARY KEY ("key"),
    CONSTRAINT FK_detail1_to_master FOREIGN KEY (id1, id2) REFERENCES keywords.master(id1, id2),
    CONSTRAINT AK_detail1_1 UNIQUE (id1, id2)
);

DROP TABLE IF EXISTS keywords.detail2 CASCADE;
CREATE TABLE keywords.detail2 (
    "key" INT,
    alt_key1 INT NULL,
    alt_key2 INT NULL,
    description VARCHAR(30),
    CONSTRAINT PK_detail2 PRIMARY KEY ("key"),
    CONSTRAINT FK_detail2_to_master FOREIGN KEY (alt_key1, alt_key2) REFERENCES keywords.master(alt_key1, alt_key2)
);

INSERT INTO keywords."UPPERCASE_MASTER" (ID, NAME, "CAST", "WHERE", "FROM", "VARCHAR", "INTEGER", "TWO WORDS", "ORDER BY") VALUES
(1, 'FIRST_RECORD', '1 CAST RESERVED WORD', '1 WHERE RESERVED WORD', '1 FROM RESERVED WORD', '1 VARCHAR RESERVED WORD', '1 INTEGER RESERVED WORD', '1 TWO WORDS FIELD NAME', '1 TWO WORDS RESERVED'),
(2, 'SECOND_RECORD', '2 CAST RESERVED WORD', '2 WHERE RESERVED WORD', '2 FROM RESERVED WORD', '2 VARCHAR RESERVED WORD', '2 INTEGER RESERVED WORD', '2 TWO WORDS FIELD NAME', '2 TWO WORDS RESERVED'),
(3, 'THIRD_RECORD', '3 CAST RESERVED WORD', '3 WHERE RESERVED WORD', '3 FROM RESERVED WORD', '3 VARCHAR RESERVED WORD', '3 INTEGER RESERVED WORD', '3 TWO WORDS FIELD NAME', '3 TWO WORDS RESERVED'),
(4, 'FOURTH_RECORD', '4 CAST RESERVED WORD', '4 WHERE RESERVED WORD', '4 FROM RESERVED WORD', '4 VARCHAR RESERVED WORD', '4 INTEGER RESERVED WORD', '4 TWO WORDS FIELD NAME', '4 TWO WORDS RESERVED'),
(5, 'FIFTH_RECORD', '5 CAST RESERVED WORD', '5 WHERE RESERVED WORD', '5 FROM RESERVED WORD', '5 VARCHAR RESERVED WORD', '5 INTEGER RESERVED WORD', '5 TWO WORDS FIELD NAME', '5 TWO WORDS RESERVED');

INSERT INTO keywords.lowercase_detail (id, master_id, "two words", "select", "as", "0 = 0 and '", result, is_active) VALUES
(1, 1, '1 two words field name', '1 select reserved word', '1 as reserved word', '1 "0 = 0 and ''" field name', 1234.56, 1),
(2, 1, '2 two words field name', '2 select reserved word', '2 as reserved word', '2 "0 = 0 and ''" field name', 2345.67, 1),
(3, 1, '3 two words field name', '3 select reserved word', '3 as reserved word', '3 "0 = 0 and ''" field name', 3456.78, 1),
(4, 3, '4 two words field name', '4 select reserved word', '4 as reserved word', '4 "0 = 0 and ''" field name', 456.78, 0),
(5, 3, '5 two words field name', '5 select reserved word', '5 as reserved word', '5 "0 = 0 and ''" field name', 567.89, 0),
(6, 3, '6 two words field name', '6 select reserved word', '6 as reserved word', '6 "0 = 0 and ''" field name', 678.90, 0),
(7, 4, '7 two words field name', '7 select reserved word', '7 as reserved word', '7 "0 = 0 and ''" field name', 789.01, 0),
(8, 5, '8 two words field name', '8 select reserved word', '8 as reserved word', '8 "0 = 0 and ''" field name', 8910.11, 1),
(9, 5, '9 two words field name', '9 select reserved word', '9 as reserved word', '9 "0 = 0 and ''" field name', 910.11, 0),
(10, 5, '10 two words field name', '10 select reserved word', '10 as reserved word', '10 "0 = 0 and ''" field name', 1011.12, 1),
(11, 5, '11 two words field name', '11 select reserved word', '11 as reserved word', '11 "0 = 0 and ''" field name', 1112.13, 0);

INSERT INTO keywords."MixedCase_1:1" ("Id", "(parentheses)", "In", "LowerCaseId") VALUES
(1, '1 (parentheses)', 1, 2),
(2, '2 (parentheses)', 1, 4),
(3, '3 (parentheses)', 1, 6),
(4, '4 (parentheses)', 1, 8),
(5, '5 (parentheses)', 1, 10);

INSERT INTO keywords."CAST" (PK_FIELD_NAME, ID, ID2, is_active, types) VALUES
(1, 1, 2, 1, '1_types_#438'),
(2, 2, 4, 0, '2_types_#438'),
(3, 3, 6, 1, '3_types_#438'),
(4, 4, 8, 0, '4_types_#438'),
(5, 5, 10, 1, '5_types_#438'),
(6, 6, 11, 0, '6_types_#438'),
(7, 7, 9, 1, '7_types_#438'),
(8, 8, 7, 0, '8_types_#438'),
(9, 9, 5, 1, '9_types_#438'),
(10, 10, 3, 0, '10_types_#438'),
(11, 11, 1, 1, '11_types_#438'),
(12, 1, 11, 0, '12_types_#438'),
(13, 2, 9, 1, '13_types_#438'),
(14, 3, 7, 0, '14_types_#438'),
(15, 4, 5, 1, '15_types_#438'),
(16, 5, 3, 0, '16_types_#438'),
(17, 6, 1, 1, '17_types_#438'),
(18, 7, 2, 0, '18_types_#438'),
(19, 8, 4, 1, '19_types_#438'), 
(20, 9, 6, 0, '20_types_#438'),
(21, 10, 8, 1, '21_types_#438'),
(22, 11, 10, 0, '22_types_#438');

INSERT INTO keywords."""QUOTED TABLE_NAME""" (ID, "`cast`", "= ""QUOTE""", "`name""[", description) VALUES
(1, 1, 1, 11, 'RECORD 1'),
(2, 2, 2, 9, 'RECORD 2'),
(3, 3, 4, 7, 'RECORD 3'),
(4, 4, 6, 5, 'RECORD 4'),
(5, 5, 8, 3, 'RECORD 5');

INSERT INTO keywords.CALCULATE (".WHERE", "LOWER", "UPPER", "LENGTH", "STARTSWITH", "ENDSWITH", "CONTAINS", "LIKE", "JOIN_STRINGS", "LPAD", "RPAD", "FIND", "STRIP", "REPLACE", "STRCOUNT", "GETPART", DATETIME, "YEAR", "QUARTER", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "DATEDIFF", "DAYOFWEEK", "DAYNAME") VALUES
(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO keywords."WHERE" (".CALCULATE", "IFF", "ISIN", DEFAULT_TO, PRESENT, "ABSENT", "KEEP_IF", "MONOTONIC", ABS, "ROUND", "CEIL", "FLOOR", "POWER", "SQRT", "SIGN", "SMALLEST", "LARGEST", "SUM", "AVG", "MEDIAN", "MIN", "MAX", "QUANTILE", "ANYTHING", "COUNT", "NDISTINCT", HAS, "HASNOT", "VAR", "STD") VALUES
(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO keywords."PARTITION" ("ORDER_BY", "CALCULATE", "WHERE", "TOP_K", "SINGULAR", "BEST", "CROSS", "RANKING", "PERCENTILE", "PREV", "NEXT", "RELSUM", "RELAVG", "RELCOUNT", "RELSIZE", "STRING", "INTEGER", "FLOAT", "NUMERIC", "DECIMAL", "CHAR", "VARCHAR") VALUES
(1, 2, 5, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL),
(2, 3, 1, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL),
(3, 4, 2, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL),
(4, 5, 3, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL),
(5, 1, 4, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL),
(6, 5, 2, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL),
(7, 4, 1, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL),
(8, 3, 5, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL),
(9, 2, 4, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL),
(10, 1, 3, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO keywords."COUNT" ("this", "class", "import", "def", "%%pydough", "node", """,", ".", "bool", "__call__", "int", "FLOAT", "__init__", "new", "del", "__col__", "__col1__", "__class__", "str", "dict", "__add__", "__mul__") VALUES
(1, 1, 1011, 2011, 3011, 4011, 5011, 6011, NULL, NULL, 8011, NULL, 7011, NULL, NULL, 10011, 11011, NULL, NULL, 9011, NULL, NULL),
(2, 1, 1021, 2021, 3021, 4021, 5021, 6021, NULL, NULL, 8021, NULL, 7021, NULL, NULL, 10021, 11021, NULL, NULL, 9021, NULL, NULL),
(3, 1, 1031, 2031, 3031, 4031, 5031, 6031, NULL, NULL, 8031, NULL, 7031, NULL, NULL, 10031, 11031, NULL, NULL, 9031, NULL, NULL),
(4, 1, 1041, 2041, 3041, 4041, 5041, 6041, NULL, NULL, 8041, NULL, 7041, NULL, NULL, 10041, 11041, NULL, NULL, 9041, NULL, NULL),
(5, 1, 1051, 2051, 3051, 4051, 5051, 6051, NULL, NULL, 8051, NULL, 7051, NULL, NULL, 10051, 11051, NULL, NULL, 9051, NULL, NULL),
(6, 1, 1061, 2061, 3061, 4061, 5061, 6061, NULL, NULL, 8061, NULL, 7061, NULL, NULL, 10061, 11061, NULL, NULL, 9061, NULL, NULL),
(7, 1, 1071, 2071, 3071, 4071, 5071, 6071, NULL, NULL, 8071, NULL, 7071, NULL, NULL, 10071, 11071, NULL, NULL, 9071, NULL, NULL),
(8, 1, 1081, 2081, 3081, 4081, 5081, 6081, NULL, NULL, 8081, NULL, 7081, NULL, NULL, 10081, 11081, NULL, NULL, 9081, NULL, NULL),
(10, 1, 1091, 2091, 3091, 4091, 5091, 6091, NULL, NULL, 8091, NULL, 7091, NULL, NULL, 10091, 11091, NULL, NULL, 9091, NULL, NULL),
(11, 1, 1101, 2101, 3101, 4101, 5101, 6101, NULL, NULL, 8101, NULL, 7101, NULL, NULL, 10101, 11101, NULL, NULL, 9101, NULL, NULL);

INSERT INTO keywords.master (id1, id2, alt_key1, alt_key2, description) VALUES
(1, 1, 1, 1, 'One-One master row'),
(1, 2, 1, 2, 'One-Two master row'),
(2, 1, 2, 1, 'Two-One master row'),
(2, 2, 2, 2, 'Two-Two master row'),
(3, 1, 3, 1, 'Three-One master row');

INSERT INTO keywords.detail1 ("key", id1, id2, description) VALUES 
(1, 1, 1, '1 One-One AK-PK'),
(2, 1, 2, '2 One-Two AK-PK'),
(3, 2, 1, '3 Two-One AK-PK'),
(4, 3, 1, '4 Three-One AK-PK');

INSERT INTO keywords.detail2 ("key", alt_key1, alt_key2, description) VALUES 
(1, 1, 1, '1 One-One FK-PK'),
(2, 1, 2, '2 One-Two FK-PK'),
(3, 2, 1, '3 Two-One FK-PK'),
(4, 3, 1, '4 Three-One FK-PK'),
(5, 3, 1, '5 Three-One FK-PK'),
(6, 1, 1, '6 One-One FK-PK'),
(7, 1, 1, '7 One-One FK-PK');
-- ACADEMIC
DROP TABLE IF EXISTS main.author CASCADE;
CREATE TABLE main.author (
  aid bigint NOT NULL,
  homepage text,
  name text,
  oid bigint
);

DROP TABLE IF EXISTS main.cite CASCADE;
CREATE TABLE main.cite (
  cited bigint,
  citing bigint
);

DROP TABLE IF EXISTS main.conference CASCADE;
CREATE TABLE main.conference (
  cid bigint NOT NULL,
  homepage text,
  name text
);

DROP TABLE IF EXISTS main.domain CASCADE;
CREATE TABLE main.domain (
  did bigint NOT NULL,
  name text
);


DROP TABLE IF EXISTS main.domain_author CASCADE;
CREATE TABLE main.domain_author (
  aid bigint NOT NULL,
  did bigint NOT NULL
);


DROP TABLE IF EXISTS main.domain_conference CASCADE;
CREATE TABLE main.domain_conference (
  cid bigint NOT NULL,
  did bigint NOT NULL
);


DROP TABLE IF EXISTS main.domain_journal CASCADE;
CREATE TABLE main.domain_journal (
  did bigint NOT NULL,
  jid bigint NOT NULL
);


DROP TABLE IF EXISTS main.domain_keyword CASCADE;
CREATE TABLE main.domain_keyword (
  did bigint NOT NULL,
  kid bigint NOT NULL
);


DROP TABLE IF EXISTS main.domain_publication CASCADE;
CREATE TABLE main.domain_publication (
  did bigint NOT NULL,
  pid bigint NOT NULL
);


DROP TABLE IF EXISTS main.journal CASCADE;
CREATE TABLE main.journal (
  homepage text,
  jid bigint NOT NULL,
  name text
);


DROP TABLE IF EXISTS main.keyword CASCADE;
CREATE TABLE main.keyword (
  keyword text,
  kid bigint NOT NULL
);


DROP TABLE IF EXISTS main.organization CASCADE;
CREATE TABLE main.organization (
  continent text,
  homepage text,
  name text,
  oid bigint NOT NULL
);


DROP TABLE IF EXISTS main.publication CASCADE;
CREATE TABLE main.publication (
  abstract text,
  cid bigint,
  citation_num bigint,
  jid bigint,
  pid bigint NOT NULL,
  reference_num bigint,
  title text,
  year bigint
);


DROP TABLE IF EXISTS main.publication_keyword CASCADE;
CREATE TABLE main.publication_keyword (
  pid bigint NOT NULL,
  kid bigint NOT NULL
);


DROP TABLE IF EXISTS main.writes CASCADE;
CREATE TABLE main.writes (
  aid bigint NOT NULL,
  pid bigint NOT NULL
);


INSERT INTO main.author (aid, homepage, name, oid) VALUES
(1, 'www.larry.com', 'Larry Summers', 2),
(2, 'www.ashish.com', 'Ashish Vaswani', 3),
(3, 'www.noam.com', 'Noam Shazeer', 3),
(4, 'www.martin.com', 'Martin Odersky', 4),
(5, NULL, 'Kempinski', NULL);


INSERT INTO main.cite (cited, citing) VALUES
(1, 2),
(1, 3),
(1, 4),
(1, 5),
(2, 3),
(2, 5),
(3, 4),
(3, 5),
(4, 5);


INSERT INTO main.conference (cid, homepage, name) VALUES
(1, 'www.isa.com', 'ISA'),
(2, 'www.aaas.com', 'AAAS'),
(3, 'www.icml.com', 'ICML');


INSERT INTO main.domain (did, name) VALUES
(1, 'Data Science'),
(2, 'Natural Sciences'),
(3, 'Computer Science'),
(4, 'Sociology'),
(5, 'Machine Learning');


INSERT INTO main.domain_author (aid, did) VALUES
(1, 2),
(1, 4),
(2, 3),
(2, 1),
(2, 5),
(3, 5),
(3, 3),
(4, 3);


INSERT INTO main.domain_conference (cid, did) VALUES
(1, 2),
(2, 4),
(3, 5);


INSERT INTO main.domain_journal (did, jid) VALUES
(1, 2),
(2, 3),
(5, 4);


INSERT INTO main.domain_keyword (did, kid) VALUES
(1, 2),
(2, 3);


INSERT INTO main.domain_publication (did, pid) VALUES
(4, 1),
(2, 2),
(1, 3),
(3, 4),
(3, 5),
(5, 5);


INSERT INTO main.journal (homepage, jid, name) VALUES
('www.aijournal.com', 1, 'Journal of Artificial Intelligence Research'),
('www.nature.com', 2, 'Nature'),
('www.science.com', 3, 'Science'),
('www.ml.com', 4, 'Journal of Machine Learning Research');


INSERT INTO main.keyword (keyword, kid) VALUES
('AI', 1),
('Neuroscience', 2),
('Machine Learning', 3),
('Keyword 4', 4);


INSERT INTO main.organization (continent, homepage, name, oid) VALUES
('Asia', 'www.organization1.com', 'Organization 1', 1),
('North America', 'www.organization2.com', 'Organization 2', 2),
('North America', 'www.organization3.com', 'Organization 3', 3),
('Europe', 'www.epfl.com', 'École Polytechnique Fédérale de Lausanne 4', 4),
('Europe', 'www.organization5.com', 'Organization 5', 5);


INSERT INTO main.publication (abstract, cid, citation_num, jid, pid, reference_num, title, year) VALUES
('Abstract 1', 1, 4, 1, 1, 0, 'The Effects of Climate Change on Agriculture', 2020),
('Abstract 2', 2, 2, 2, 2, 1, 'A Study on the Effects of Social Media on Mental Health', 2020),
('Abstract 3', 3, 2, 2, 3, 2, 'Data Mining Techniques', 2021),
('Abstract 4', 3, 1, 2, 4, 2, 'Optimizing GPU Throughput', 2021),
('Abstract 5', 3, 0, 4, 5, 4, 'Attention is all you need', 2021);


INSERT INTO main.publication_keyword (pid, kid) VALUES
(1, 2),
(2, 3);


INSERT INTO main.writes (aid, pid) VALUES
(1, 1),
(1, 2),
(2, 3),
(2, 4),
(2, 5),
(3, 5);
