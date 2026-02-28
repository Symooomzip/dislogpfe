-- Dislog PFE Data Warehouse Star Schema Design
-- Target: SQL Server

-- =========================================
-- DIMENSION TABLES
-- =========================================

CREATE TABLE DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    AccountID NVARCHAR(50) NOT NULL,
    AccountName NVARCHAR(100),
    RegionID NVARCHAR(50),
    RegionDescription NVARCHAR(100),
    SectorID NVARCHAR(50),
    SectorDescription NVARCHAR(100),
    -- Slowly Changing Dimension (SCD) Type 2 fields (optional, but good practice)
    ValidFrom DATE DEFAULT GETDATE(),
    ValidTo DATE,
    IsCurrent BIT DEFAULT 1
);

CREATE TABLE DimSeller (
    SellerKey INT IDENTITY(1,1) PRIMARY KEY,
    SellerID NVARCHAR(50) NOT NULL,
    SellerName NVARCHAR(100),
    ValidFrom DATE DEFAULT GETDATE(),
    ValidTo DATE,
    IsCurrent BIT DEFAULT 1
);

CREATE TABLE DimProduct (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    ItemID NVARCHAR(50) NOT NULL,
    ProductName NVARCHAR(100),
    NameAlias NVARCHAR(100),
    Brand NVARCHAR(50),  -- mapped from 'marque'
    ValidFrom DATE DEFAULT GETDATE(),
    ValidTo DATE,
    IsCurrent BIT DEFAULT 1
);

CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY, -- format YYYYMMDD
    FullDate DATE NOT NULL,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT,
    DayOfWeek INT,
    DayName NVARCHAR(20),
    MonthName NVARCHAR(20),
    IsWeekend BIT
);

CREATE TABLE DimPromotion (
    PromotionKey INT IDENTITY(1,1) PRIMARY KEY,
    PromoType NVARCHAR(50) NOT NULL -- 'Discount', 'Gift', 'No Promotion', 'Rebates'
);

CREATE TABLE DimPaymentMethod (
    PaymentMethodKey INT IDENTITY(1,1) PRIMARY KEY,
    PaymentMethodCode NVARCHAR(10) NOT NULL, -- e.g., '1', '102', '205'
    PaymentMethodDescription NVARCHAR(50) 
);

-- =========================================
-- FACT TABLES
-- =========================================

-- FactSales combines SalesHeader and SalesLine at the lowest grain (Line level)
CREATE TABLE FactSales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    SaleID BIGINT NOT NULL, -- Degenerate dimension for header grouping
    
    -- Foreign Keys to Dimensions
    OrderDateKey INT FOREIGN KEY REFERENCES DimDate(DateKey),
    DeliveryDateKey INT FOREIGN KEY REFERENCES DimDate(DateKey),
    CustomerKey INT FOREIGN KEY REFERENCES DimCustomer(CustomerKey),
    SellerKey INT FOREIGN KEY REFERENCES DimSeller(SellerKey),
    ProductKey INT FOREIGN KEY REFERENCES DimProduct(ProductKey),
    PromotionKey INT FOREIGN KEY REFERENCES DimPromotion(PromotionKey),
    
    -- Measures (Strictly Line Level to avoid Double Counting)
    Quantity INT,
    UnitPrice DECIMAL(18, 2),
    
    -- We use Line Amounts. (Header amounts should be aggregated from these)
    LineBruteAmount DECIMAL(18, 2),
    LineDiscountAmount DECIMAL(18, 2),
    LineNetAmount DECIMAL(18, 2),
    LineTaxAmount DECIMAL(18, 2),
    LineTotalAmount DECIMAL(18, 2)
);

-- FactInvoices captures the payments made against sales
CREATE TABLE FactInvoices (
    InvoiceKey INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceID NVARCHAR(50) NOT NULL, -- Degenerate dimension
    SaleID BIGINT NOT NULL, -- Links back to the sale (could also link to FactSales via a bridge or directly if 1-1)
    
    -- Foreign Keys to Dimensions
    PaymentDateKey INT FOREIGN KEY REFERENCES DimDate(DateKey), -- Assuming we derive this or use order date
    CustomerKey INT FOREIGN KEY REFERENCES DimCustomer(CustomerKey),
    PaymentMethodKey INT FOREIGN KEY REFERENCES DimPaymentMethod(PaymentMethodKey),
    
    -- Measures
    PaymentAmount DECIMAL(18, 2)
);
