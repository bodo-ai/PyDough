-- Custom SQL schema to initialize a custom TechnoGraph database with tables for
-- products, devices, countries, users, incidents and errors.

CREATE TABLE PRODUCTS (
  pr_id BIGINT NOT NULL PRIMARY KEY,
  pr_name VARCHAR(30) NOT NULL,
  pr_type VARCHAR(20) NOT NULL,
  pr_brand VARCHAR(20) NOT NULL,
  pr_prodcost REAL(10, 2) NOT NULL,
  pr_release DATE NOT NULL
);

CREATE TABLE DEVICES (
  de_id BIGINT NOT NULL PRIMARY KEY,
  de_product_id BIGINT NOT NULL,
  de_purchase_country_id BIGINT NOT NULL,
  de_production_country_id BIGINT NOT NULL,
  de_owner_id BIGINT NOT NULL,
  de_purchase_ts TIMESTAMP NOT NULL,
  de_purchase_cost REAL(10, 2) NOT NULL,
  de_purchase_tax REAL(10, 2) NOT NULL,
  de_warranty_type VARCHAR(30),
  de_warranty_ex TIMESTAMP
);

CREATE TABLE COUNTRIES (
  co_id BIGINT NOT NULL PRIMARY KEY,
  co_name VARCHAR(20) NOT NULL
);

CREATE TABLE USERS(
  us_id BIGINT NOT NULL PRIMARY KEY,
  us_username VARCHAR (30) NOT NULL,
  us_country_id BIGINT NOT NULL,
  us_birthdate DATE NOT NULL,
  us_account_creation_date DATE NOT NULL
);

CREATE TABLE INCIDENTS(
  in_id BIGINT NOT NULL PRIMARY KEY,
  in_device_id BIGINT NOT NULL,
  in_repair_country_id BIGINT NOT NULL,
  in_error_id BIGINT NOT NULL,
  in_error_report_ts TIMESTAMP NOT NULL,
  in_error_repair_cost REAL(10, 2) NOT NULL
);

CREATE TABLE ERRORS(
  er_id BIGINT NOT NULL PRIMARY KEY,
  er_name VARCHAR(30) NOT NULL,
  er_type VARCHAR(20) NOT NULL
);
