CREATE TABLE Region (
    regionid NVARCHAR(50) PRIMARY KEY,
    description NVARCHAR(100)
);

CREATE TABLE Sector (
    sectorid NVARCHAR(50) PRIMARY KEY,
    description NVARCHAR(100)
);

CREATE TABLE Customer (
    accountid NVARCHAR(50) PRIMARY KEY,
    accountname NVARCHAR(100),
    regionid NVARCHAR(50),
    sectorid NVARCHAR(50),
    FOREIGN KEY (regionid) REFERENCES Region(regionid),
    FOREIGN KEY (sectorid) REFERENCES Sector(sectorid)
);

CREATE TABLE Seller (
    sellerid NVARCHAR(50) PRIMARY KEY,
    sellername NVARCHAR(100)
);

CREATE TABLE Product (
    itemid NVARCHAR(50) PRIMARY KEY,
    name NVARCHAR(100),
    namealias NVARCHAR(100),
    marque NVARCHAR(50)
);

CREATE TABLE SalesHeader (
    saleid BIGINT PRIMARY KEY,
    accountid NVARCHAR(50),
    sellerid NVARCHAR(50),
    orderdate DATE,
    delivdate DATE,
    bruteamount DECIMAL(18, 2),
    netamount DECIMAL(18, 2),
    taxamount DECIMAL(18, 2),
    totalamount DECIMAL(18, 2),
    FOREIGN KEY (accountid) REFERENCES Customer(accountid),
    FOREIGN KEY (sellerid) REFERENCES Seller(sellerid)
);

CREATE TABLE SalesLine (
    saleid BIGINT,
    itemid NVARCHAR(50),
    qty INT,
    unitprice DECIMAL(18, 2),
    httotalamount DECIMAL(18, 2),
    ttctotalamount DECIMAL(18, 2),
    promotype NVARCHAR(50),
    promovalue DECIMAL(18, 2),
    PRIMARY KEY (saleid, itemid),
    FOREIGN KEY (saleid) REFERENCES SalesHeader(saleid),
    FOREIGN KEY (itemid) REFERENCES Product(itemid)
);

CREATE TABLE Invoice (
    invoiceid NVARCHAR(50) PRIMARY KEY,
    salesid BIGINT,
    paymentamount DECIMAL(18, 2),
    paymentmethod NVARCHAR(3),
    FOREIGN KEY (salesid) REFERENCES SalesHeader(saleid)
);