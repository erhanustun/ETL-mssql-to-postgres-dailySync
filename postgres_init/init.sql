DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    OrderID         INT PRIMARY KEY,
    UserID          INT NOT NULL,
    AddedToCartAt TIMESTAMP,
    OrderCreatedAt TIMESTAMP,
    Amount           NUMERIC(10, 2) NOT NULL,
    Product          VARCHAR(100) NOT NULL,
    IsDelivered     BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_order_created_at ON orders(OrderCreatedAt);

DROP TABLE IF EXISTS incomplete_orders;
CREATE TABLE incomplete_orders (
    OrderID         INT PRIMARY KEY,
    UserID          INT NOT NULL,
    AddedToCartAt TIMESTAMP,
    OrderCreatedAt TIMESTAMP NULL, -- Bu sütun NULL olabilir
    Amount          NUMERIC(10, 2) NOT NULL,
    Product         VARCHAR(100) NOT NULL,
    IsDelivered     BOOLEAN NOT NULL,
    loaded_at_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- ETL tarafından yüklendiği zaman
);