CREATE TABLE sale (
    sale_date   DATE         NOT NULL,
    store       VARCHAR(32)  NOT NULL,
    product     VARCHAR(32)  NOT NULL,
    quantity    INT          NOT NULL,
    amount_cents BIGINT      NOT NULL
);
