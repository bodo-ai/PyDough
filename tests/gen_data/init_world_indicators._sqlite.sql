-- Modified version of the world development indicators dataset for e2e tests

CREATE TABLE IF NOT EXISTS "Country" (
    CountryCode                                TEXT not null primary key,
    ShortName                                  TEXT,
    TableName                                  TEXT,
    LongName                                   TEXT,
    Alpha2Code                                 TEXT,
    CurrencyUnit                               TEXT,
    SpecialNotes                               TEXT,
    Region                                     TEXT,
    IncomeGroup                                TEXT,
    Wb2Code                                    TEXT,
    NationalAccountsBaseYear                   TEXT,
    NationalAccountsReferenceYear              TEXT,
    SnaPriceValuation                          TEXT,
    LendingCategory                            TEXT,
    OtherGroups                                TEXT,
    SystemOfNationalAccounts                   TEXT,
    AlternativeConversionFactor                TEXT,
    PppSurveyYear                              TEXT,
    BalanceOfPaymentsManualInUse               TEXT,
    ExternalDebtReportingStatus                TEXT,
    SystemOfTrade                              TEXT,
    GovernmentAccountingConcept                TEXT,
    ImfDataDisseminationStandard               TEXT,
    LatestPopulationCensus                     TEXT,
    LatestHouseholdSurvey                      TEXT,
    SourceOfMostRecentIncomeAndExpenditureData TEXT,
    VitalRegistrationComplete                  TEXT,
    LatestAgriculturalCensus                   TEXT,
    LatestIndustrialData                       INTEGER,
    LatestTradeData                            INTEGER,
    LatestWaterWithdrawalData                  INTEGER
);

CREATE TABLE IF NOT EXISTS "Series" (
    SeriesCode                       TEXT not null primary key,
    Topic                            TEXT,
    IndicatorName                    TEXT,
    ShortDefinition                  TEXT,
    LongDefinition                   TEXT,
    UnitOfMeasure                    TEXT,
    Periodicity                      TEXT,
    BasePeriod                       TEXT,
    OtherNotes                       INTEGER,
    AggregationMethod                TEXT,
    LimitationsAndExceptions         TEXT,
    NotesFromOriginalSource          TEXT,
    GeneralComments                  TEXT,
    Source                           TEXT,
    StatisticalConceptAndMethodology TEXT,
    DevelopmentRelevance             TEXT,
    RelatedSourceLinks               TEXT,
    OtherWebLinks                    INTEGER,
    RelatedIndicators                INTEGER,
    LicenseType                      TEXT
);

CREATE TABLE CountryNotes (
    Countrycode TEXT NOT NULL ,
    Seriescode  TEXT NOT NULL ,
    Description TEXT,
    primary key (Countrycode, Seriescode),
    FOREIGN KEY (Seriescode) REFERENCES Series(SeriesCode),
    FOREIGN KEY (Countrycode) REFERENCES Country(CountryCode)
);

CREATE TABLE Footnotes (
    Countrycode TEXT NOT NULL ,
    Seriescode  TEXT NOT NULL ,
    Year        TEXT,
    Description TEXT,
    primary key (Countrycode, Seriescode, Year),
    FOREIGN KEY (Seriescode) REFERENCES Series(SeriesCode),
    FOREIGN KEY (Countrycode) REFERENCES Country(CountryCode)
);

CREATE TABLE Indicators (
    CountryName   TEXT,
    CountryCode   TEXT NOT NULL ,
    IndicatorName TEXT,
    IndicatorCode TEXT NOT NULL ,
    Year          INTEGER NOT NULL ,
    Value         INTEGER,
    primary key (CountryCode, IndicatorCode, Year),
    FOREIGN KEY (CountryCode) REFERENCES Country(CountryCode)
);

CREATE TABLE SeriesNotes (
    Seriescode  TEXT not null ,
    Year        TEXT not null ,
    Description TEXT,
    primary key (Seriescode, Year),
    foreign key (Seriescode) references Series(SeriesCode)
);
