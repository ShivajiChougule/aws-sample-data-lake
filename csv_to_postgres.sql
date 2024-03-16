CREATE TABLE CUSTOMERS (
    CUSTOMER_ID INTEGER,
    CUSTOMER_NAME VARCHAR(100),
    CUSTOMER_CLASS VARCHAR(50),
    CITY VARCHAR(100),
    STATE VARCHAR(50),
    ZIP VARCHAR(20),
    EMAIL VARCHAR(100),
    PHONE VARCHAR(20)
);

CREATE TABLE INVENTORY (
    INVENTORY_ID INTEGER,
    PRODUCT_GROUP_ID INTEGER,
    QUANTITY_AVAILABLE INTEGER,
    WAREHOUSE_ID INTEGER
);

CREATE TABLE WAREHOUSE (
    WAREHOUSE_ID INTEGER,
    WAREHOUSE_NAME VARCHAR(100),
    IS_REFRIGERATED BOOLEAN,
    STORE_LOCATION_ID INTEGER
);


CREATE TABLE STORE_MASTER_DATA (
    STORE_LOCATION_ID INTEGER,
    STORE_CITY VARCHAR(100),
    STORE_STATE VARCHAR(100)
);

CREATE TABLE PRODUCT_MASTER (
    PRODUCT_ID INTEGER,
    PROD_CODE VARCHAR(50),
    PROD_DESC VARCHAR(100),
    PRODUCT_GROUP_ID INTEGER,
    UNIT_PRICE DECIMAL(10,2)
);

CREATE TABLE PRODUCT_GROUP_MASTER (
    PRODUCT_GROUP_ID INTEGER,
    PRODUCT_GROUP_NAME VARCHAR(100)
);

---run below lines in plsql

\copy CUSTOMERS FROM 'C:/Users/shivaji.choughule/Downloads/CSVdatasets_Akshay/CUSTOMERS.csv' WITH (FORMAT CSV, HEADER);
\copy INVENTORY FROM 'C:/Users/shivaji.choughule/Downloads/CSVdatasets_Akshay/INVENTORY.csv' WITH (FORMAT CSV, HEADER);
\copy WAREHOUSE FROM 'C:/Users/shivaji.choughule/Downloads/CSVdatasets_Akshay/WAREHOUSE.csv' WITH (FORMAT CSV, HEADER);
\copy STORE_MASTER_DATA FROM 'C:/Users/shivaji.choughule/Downloads/CSVdatasets_Akshay/STORE_MASTER_DATA.csv' WITH (FORMAT CSV, HEADER);
\copy PRODUCT_MASTER FROM 'C:/Users/shivaji.choughule/Downloads/CSVdatasets_Akshay/PRODUCT_MASTER.csv' WITH (FORMAT CSV, HEADER);
\copy PRODUCT_GROUP_MASTER FROM 'C:/Users/shivaji.choughule/Downloads/CSVdatasets_Akshay/PRODUCT_GROUP_MASTER.csv' WITH (FORMAT CSV, HEADER);



