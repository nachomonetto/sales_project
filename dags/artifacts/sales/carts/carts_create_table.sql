CREATE TABLE IF NOT EXISTS CARTS (
    ID INT NOT NULL,
    USERID INT NOT NULL,
    DATE TIMESTAMPTZ NOT NULL,
    PRODUCTS VARCHAR(5000) NULL,
    __V INT NOT NULL,
    PROCESS_DATE DATE NOT NULL,
    LOAD_DATE TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);