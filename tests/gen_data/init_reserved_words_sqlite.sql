CREATE TABLE UPPERCASE_MASTER (
    ID BIGINT NOT NULL PRIMARY KEY,
    NAME VARCHAR(30) NOT NULL,
    "CAST" VARCHAR(30),
    "WHERE" VARCHAR(30),
    "FROM" VARCHAR(30),
    "VARCHAR" VARCHAR(30),
    "INTEGER" INTEGER,
    "TWO WORDS" VARCHAR(30),
    "ORDER BY" VARCHAR(30)
);

CREATE TABLE lowercase_detail (
    id BIGINT NOT NULL PRIMARY KEY,
    master_id BIGINT NOT NULL,
    "two words" VARCHAR(30),
    "select" VARCHAR(30),
    "as" VARCHAR(30),
    "0 = 0 and '" varchar(30),
    result NUMERIC(10,2),
    is_active tinyint,
    CONSTRAINT lowercase_to_UPPERCASE FOREIGN KEY (master_id) REFERENCES UPPERCASE_MASTER(ID)
);

CREATE TABLE "MixedCase_1:1" (
    Id BIGINT NOT NULL PRIMARY KEY,
    "(parentheses)" VARCHAR(30),
    "In" tinyint,
    LowerCaseId BIGINT NOT NULL,
    CONSTRAINT "MixedCase_1:1_to_UPPERCASE" FOREIGN KEY (Id) REFERENCES UPPERCASE_MASTER(ID)  
    CONSTRAINT "MixedCase_1:1_to_lowercase_detail" FOREIGN KEY (LowerCaseId) REFERENCES lowercase_detail(id)
);

CREATE TABLE "CAST" (
    PK_FIELD_NAME BIGINT NOT NULL PRIMARY KEY,
    ID BIGINT NOT NULL,
    ID2 BIGINT,
    is_active tinyint,
    CONSTRAINT CAST_to_lowercase_detail1 FOREIGN KEY (ID) REFERENCES lowercase_detail(id),
    CONSTRAINT CAST_to_lowercase_detail2 FOREIGN KEY (ID2) REFERENCES lowercase_detail(id)
);

CREATE TABLE """QUOTED TABLE_NAME""" (
    ID BIGINT NOT NULL PRIMARY KEY,
    "`cast`" BIGINT NOT NULL,
    "= ""QUOTE""" BIGINT NOT NULL,
    "`name""[" BIGINT NOT NULL,
    description VARCHAR(30),
    CONSTRAINT QUOTED_UNIQUE UNIQUE ("`cast`"),
    CONSTRAINT QUOTED_to_UPPERCASE_MASTER FOREIGN KEY ("`cast`") REFERENCES UPPERCASE_MASTER(ID),
    CONSTRAINT QUOTED_to_lowercase_detail1 FOREIGN KEY ("= ""QUOTE""") REFERENCES lowercase_detail(id)
    CONSTRAINT QUOTED_to_lowercase_detail2 FOREIGN KEY ("`name""[") REFERENCES lowercase_detail(id)
);

CREATE TABLE "CALCULATE" (
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
    "DATETIME" INTEGER,
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

CREATE TABLE "WHERE" (
    ".CALCULATE" BIGINT NOT NULL PRIMARY KEY,
    "IFF" INTEGER,
    "ISIN" INTEGER,
    "DEFAULT_TO" INTEGER,
    "PRESENT" INTEGER,
    "ABSENT" INTEGER,
    "KEEP_IF" INTEGER,
    "MONOTONIC" INTEGER,
    "ABS" INTEGER,
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
    "HAS" INTEGER,
    "HASNOT" INTEGER,
    "VAR" INTEGER,
    "STD" INTEGER,
    CONSTRAINT WHERE_to_CALCULATE FOREIGN KEY (".CALCULATE") REFERENCES "CALCULATE"(".WHERE")
);

CREATE TABLE "PARTITION" (
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
    CONSTRAINT PARTITION_to_CALCULATE FOREIGN KEY ("CALCULATE") REFERENCES "CALCULATE"(".WHERE"),
    CONSTRAINT PARTITION_to_WHERE FOREIGN KEY ("WHERE") REFERENCES "WHERE"(".CALCULATE")
);

CREATE TABLE "COUNT" (
    "this" BIGINT NOT NULL,
    "class" BIGINT NOT NULL,
    "import" INTEGER,
    "def" INTEGER,
    "%%pydough" INTEGER,
    "%load_ext" INTEGER,
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
    CONSTRAINT PK_COUNT PRIMARY KEY ("this","class"),
    CONSTRAINT COUNT_to_CAST FOREIGN KEY ("this") REFERENCES "CAST"(PK_FIELD_NAME)
);

CREATE TABLE master (
    ID1 INT NOT NULL,
    ID2 INT NOT NULL,
    ALT_KEY1 INT NOT NULL,
    ALT_KEY2 INT NOT NULL,
    DESCRIPTION VARCHAR(30),
    CONSTRAINT PK_master PRIMARY KEY (ID1, ID2),
    CONSTRAINT AK_master_1 UNIQUE (ALT_KEY1, ALT_KEY2)
);

CREATE TABLE detail1 (
    "key" INT,
    id1 INT NOT NULL,
    id2 INT NOT NULL,
    description VARCHAR(30),
    CONSTRAINT PK_detail1 PRIMARY KEY ("key"),
    CONSTRAINT FK_detail1_to_master FOREIGN KEY (id1, id2) REFERENCES master(ID1, ID2)
    CONSTRAINT AK_detail1_1 UNIQUE (id1, id2)
);

CREATE TABLE detail2 (
    "key" INT,
    alt_key1 INT NULL,
    alt_key2 INT NULL,
    description VARCHAR(30),
    CONSTRAINT PK_detail2 PRIMARY KEY ("key"),
    CONSTRAINT FK_detail2_to_master FOREIGN KEY (alt_key1, alt_key2) REFERENCES master(ALT_KEY1, ALT_KEY2)
);



INSERT INTO UPPERCASE_MASTER (ID, NAME, "CAST", "WHERE", "FROM", "VARCHAR", "INTEGER", "TWO WORDS", "ORDER BY") VALUES
(1, 'FIRST_RECORD', '1 CAST RESERVED WORD', '1 WHERE RESERVED WORD', '1 FROM RESERVED WORD', '1 VARCHAR RESERVED WORD', '1 INTEGER RESERVED WORD', '1 TWO WORDS FIELD NAME', '1 TWO WORDS RESERVED'),
(2, 'SECOND_RECORD', '2 CAST RESERVED WORD', '2 WHERE RESERVED WORD', '2 FROM RESERVED WORD', '2 VARCHAR RESERVED WORD', '2 INTEGER RESERVED WORD', '2 TWO WORDS FIELD NAME', '2 TWO WORDS RESERVED'),
(3, 'THIRD_RECORD', '3 CAST RESERVED WORD', '3 WHERE RESERVED WORD', '3 FROM RESERVED WORD', '3 VARCHAR RESERVED WORD', '3 INTEGER RESERVED WORD', '3 TWO WORDS FIELD NAME', '3 TWO WORDS RESERVED'),
(4, 'FOURTH_RECORD', '4 CAST RESERVED WORD', '4 WHERE RESERVED WORD', '4 FROM RESERVED WORD', '4 VARCHAR RESERVED WORD', '4 INTEGER RESERVED WORD', '4 TWO WORDS FIELD NAME', '4 TWO WORDS RESERVED'),
(5, 'FIFTH_RECORD', '5 CAST RESERVED WORD', '5 WHERE RESERVED WORD', '5 FROM RESERVED WORD', '5 VARCHAR RESERVED WORD', '5 INTEGER RESERVED WORD', '5 TWO WORDS FIELD NAME', '5 TWO WORDS RESERVED');

INSERT INTO lowercase_detail (id, master_id, "two words", "select", "as", "0 = 0 and '", result, is_active) VALUES
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

INSERT INTO "MixedCase_1:1" (Id, "(parentheses)", "In", LowerCaseId) VALUES
(1, '1 (parentheses)', 1, 2),
(2, '2 (parentheses)', 1, 4),
(3, '3 (parentheses)', 1, 6),
(4, '4 (parentheses)', 1, 8),
(5, '5 (parentheses)', 1, 10);

INSERT INTO "CAST" (PK_FIELD_NAME, ID, ID2, is_active) VALUES
(1, 1, 2, 1),
(2, 2, 4, 0),
(3, 3, 6, 1),
(4, 4, 8, 0),
(5, 5, 10, 1),
(6, 6, 11, 0),
(7, 7, 9, 1),
(8, 8, 7, 0),
(9, 9, 5, 1),
(10, 10, 3, 0),
(11, 11, 1, 1),
(12, 1, 11, 0),
(13, 2, 9, 1),
(14, 3, 7, 0),
(15, 4, 5, 1),
(16, 5, 3, 0),
(17, 6, 1, 1),
(18, 7, 2, 0),
(19, 8, 4, 1),
(20, 9, 6, 0),
(21, 10, 8, 1),
(22, 11, 10, 0);

INSERT INTO """QUOTED TABLE_NAME""" (ID, "`cast`", "= ""QUOTE""", "`name""[", description) VALUES
(1, 1, 1, 11, 'RECORD 1'),
(2, 2, 2, 9, 'RECORD 2'),
(3, 3, 4, 7, 'RECORD 3'),
(4, 4, 6, 5, 'RECORD 4'),
(5, 5, 8, 3, 'RECORD 5');

INSERT INTO "CALCULATE" (".WHERE") VALUES
(1),
(2),
(3),
(4),
(5);

INSERT INTO "WHERE" (".CALCULATE") VALUES
(1),
(2),
(3),
(4),
(5);

INSERT INTO "PARTITION" ("ORDER_BY", "CALCULATE", "WHERE", "TOP_K", "SINGULAR", "STRING") VALUES
(1, 2, 5, 1, 1, 1),
(2, 3, 1, 1, 1, 1),
(3, 4, 2, 1, 1, 1),
(4, 5, 3, 1, 1, 1),
(5, 1, 4, 1, 1, 1),
(6, 5, 2, 1, 1, 1),
(7, 4, 1, 1, 1, 1),
(8, 3, 5, 1, 1, 1),
(9, 2, 4, 1, 1, 1),
(10, 1, 3, 1, 1, 1);

INSERT INTO "COUNT" ("this", "class", "import", "def", "%%pydough", "%load_ext", """,", ".", "__init__", "int", "dict", "__col__", "__col1__") VALUES
(1, 1, 1011, 2011, 3011, 4011, 5011, 6011, 7011, 8011, 9011, 10011, 11011),
(2, 1, 1021, 2021, 3021, 4021, 5021, 6021, 7021, 8021, 9021, 10021, 11021),
(3, 1, 1031, 2031, 3031, 4031, 5031, 6031, 7031, 8031, 9031, 10031, 11031),
(4, 1, 1041, 2041, 3041, 4041, 5041, 6041, 7041, 8041, 9041, 10041, 11041),
(5, 1, 1051, 2051, 3051, 4051, 5051, 6051, 7051, 8051, 9051, 10051, 11051),
(6, 1, 1061, 2061, 3061, 4061, 5061, 6061, 7061, 8061, 9061, 10061, 11061),
(7, 1, 1071, 2071, 3071, 4071, 5071, 6071, 7071, 8071, 9071, 10071, 11071),
(8, 1, 1081, 2081, 3081, 4081, 5081, 6081, 7081, 8081, 9081, 10081, 11081),
(10, 1, 1091, 2091, 3091, 4091, 5091, 6091, 7091, 8091, 9091, 10091, 11091),
(11, 1, 1101, 2101, 3101, 4101, 5101, 6101, 7101, 8101, 9101, 10101, 11101);

INSERT INTO master (ID1, ID2, ALT_KEY1, ALT_KEY2, DESCRIPTION) VALUES
(1, 1, 1, 1, 'One-One master row'),
(1, 2, 1, 2, 'One-Two master row'),
(2, 1, 2, 1, 'Two-One master row'),
(2, 2, 2, 2, 'Two-Two master row'),
(3, 1, 3, 1, 'Three-One master row');

INSERT INTO detail1 ("key", id1, id2, description) VALUES 
(1, 1, 1, '1 One-One AK-PK'),
(2, 1, 2, '2 One-Two AK-PK'),
(3, 2, 1, '3 Two-One AK-PK'),
(4, 3, 1, '4 Three-One AK-PK');

INSERT INTO detail2 ("key", alt_key1, alt_key2, description) VALUES 
(1, 1, 1, '1 One-One FK-PK'),
(2, 1, 2, '2 One-Two FK-PK'),
(3, 2, 1, '3 Two-One FK-PK'),
(4, 3, 1, '4 Three-One FK-PK'),
(5, 3, 1, '5 Three-One FK-PK'),
(6, 1, 1, '6 One-One FK-PK'),
(7, 1, 1, '7 One-One FK-PK');
