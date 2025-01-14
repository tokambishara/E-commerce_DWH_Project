Create table Dim_feedback
(
feedback_sk int identity(1,1) primary key,
feedback_id varchar(50) ,
feedback_score int,
)
-------------------------------------------------------------------
create table Dim_seller
(
seller_id_sk int identity(1,1) primary key,
seller_id varchar(50) ,
seller_zip_code int,
seller_city varchar(50),
seller_state varchar(50)
)
-------------------------------------------------------------------
create table [Dim_product]
(
product_id_sk int identity(1,1) primary key,
product_id varchar(50),
product_category varchar(50),
product_name_lenght int,
product_description_lenght int,
product_photos_qty int,
product_weight_g int,
product_length_cm int,
product_height_cm int,
product_width_cm int
)
-------------------------------------------------------------------

create table [Dim_orderUser_payment]
(
order_id_sk int identity(1,1) primary key,
order_id varchar(50),
order_state varchar(50),
customer_id varchar(50) ,
customer_zip_code int,
customer_city varchar(50),
customer_state varchar(50),
payment_sequential int,
payment_type  varchar(50),
payment_installments int,
payment_value int
)

-------------------------------------------------------------------

CREATE TABLE Dim_Date (
    DateKey INT PRIMARY KEY,
    DateFull Date,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT,
    DayOfWeek INT,
    DayName nvarchar(10),
    MonthName nvarchar(10)
);


DECLARE @StartDate DATE = '2016-01-01';
DECLARE @EndDate DATE = '2020-12-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO Dim_Date (
        DateKey,
        DateFull,
        Year,
        Quarter,
        Month,
        Day,
        DayOfWeek,
        DayName,
        MonthName
    )
    VALUES (
        CAST(YEAR(@StartDate) AS VARCHAR(4)) + 
        RIGHT('0' + CAST(MONTH(@StartDate) AS VARCHAR(2)), 2) + 
        RIGHT('0' + CAST(DAY(@StartDate) AS VARCHAR(2)), 2), -- DateKey
        @StartDate,                                         -- DateFull
        YEAR(@StartDate),                                   -- Year
        DATEPART(QUARTER, @StartDate),                      -- Quarter
        MONTH(@StartDate),                                  -- Month
        DAY(@StartDate),                                    -- Day
        ((DATEPART(WEEKDAY, @StartDate) + @@DATEFIRST - 1) % 7) + 1, -- DayOfWeek
        DATENAME(WEEKDAY, @StartDate),                      -- DayName
        DATENAME(MONTH, @StartDate)                         -- MonthName
    );

    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END;

-------------------------------------------------------------------
-- Create the Dim_Time table
CREATE TABLE Dim_Time (
    TimeKey INT PRIMARY KEY,       -- A unique integer key for each time
    TimeValue TIME NOT NULL,       -- The actual time value (HH:MM:SS)
    Hour INT NOT NULL,             -- The hour (0-23)
    Minute INT NOT NULL,           -- The minute (0-59)
    Second INT NOT NULL,           -- The second (0-59)
    Period AS (                    -- Computed column for AM/PM
        CASE 
            WHEN Hour >= 12 THEN 'PM'
            ELSE 'AM'
        END
    )
);

-- Populate the Dim_Time table with all possible time values
DECLARE @Hour INT = 0;
DECLARE @Minute INT;
DECLARE @Second INT;

WHILE @Hour < 24
BEGIN
    SET @Minute = 0;
    WHILE @Minute < 60
    BEGIN
        SET @Second = 0;
        WHILE @Second < 60
        BEGIN
            INSERT INTO Dim_Time (TimeKey, TimeValue, Hour, Minute, Second)
            VALUES (
                @Hour * 10000 + @Minute * 100 + @Second, -- TimeKey format: HHMMSS
                CAST(@Hour AS VARCHAR(2)) + ':' +
                RIGHT('00' + CAST(@Minute AS VARCHAR(2)), 2) + ':' +
                RIGHT('00' + CAST(@Second AS VARCHAR(2)), 2),
                @Hour,
                @Minute,
                @Second
            );

            SET @Second = @Second + 1;
        END
        SET @Minute = @Minute + 1;
    END
    SET @Hour = @Hour + 1;
END;
-------------------------------------------------------------------
create table Fact_orders
(
FactID INT IDENTITY(1,1) PRIMARY KEY,
orderuser_fk int FOREIGN KEY REFERENCES dbo.Dim_orderUser_payment(order_id_sk),
orderitem_ID INT , --FOREIGN KEY REFERENCES Dim_order_item(orderitem_id_sk),
product_fk INT FOREIGN KEY REFERENCES [Dim_product](product_id_sk),
feedback_fk INT FOREIGN KEY REFERENCES Dim_feedback(feedback_sk),
seller_fk INT FOREIGN KEY REFERENCES Dim_seller(seller_id_sk),
feedback_form_sent_date INT FOREIGN KEY REFERENCES dbo.Dim_Date(DateKey),
feedback_answer_date INT FOREIGN KEY REFERENCES dbo.Dim_Date(DateKey),
order_date INT FOREIGN KEY REFERENCES dbo.Dim_Date(DateKey),
order_approved_date INT FOREIGN KEY REFERENCES dbo.Dim_Date(DateKey),
pickup_date INT FOREIGN KEY REFERENCES dbo.Dim_Date(DateKey),
delivery_date INT FOREIGN KEY REFERENCES dbo.Dim_Date(DateKey),
estimated_date_delivery  INT FOREIGN KEY REFERENCES dbo.Dim_Date(DateKey),
pickup_limit_date INT FOREIGN KEY REFERENCES dbo.Dim_Date(DateKey),
order_time INT FOREIGN KEY REFERENCES dbo.Dim_Time(TimeKey),
order_approved_time INT FOREIGN KEY REFERENCES dbo.Dim_Time(TimeKey),
pickup_time INT FOREIGN KEY REFERENCES dbo.Dim_Time(TimeKey),
delivery_time INT FOREIGN KEY REFERENCES dbo.Dim_Time(TimeKey),
estimated_time_delivery  INT FOREIGN KEY REFERENCES dbo.Dim_Time(TimeKey),
pickup_limit_time INT FOREIGN KEY REFERENCES dbo.Dim_Time(TimeKey),
price INT,
shipping_cost INT,
)






