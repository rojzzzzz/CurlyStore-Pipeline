CREATE TABLE curly_store.processed_events (
    event_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    event_type VARCHAR(32) NOT NULL,
    occurred_at DATETIME2(3) NOT NULL,
    processed_at DATETIME2(3) NOT NULL
        DEFAULT SYSUTCDATETIME(),

    CONSTRAINT CK_processed_events_type CHECK (
        event_type IN ('order', 'refund')
    )
);

CREATE TABLE curly_store.orders (
    order_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    event_id UNIQUEIDENTIFIER NOT NULL UNIQUE,

    store_id INT NOT NULL,
    product_id INT NOT NULL,

    quantity INT NOT NULL,
    total_price DECIMAL(19, 2) NOT NULL,
    order_date DATE NOT NULL,

    status VARCHAR(24) NOT NULL DEFAULT 'completed',


    CONSTRAINT FK_orders_event FOREIGN KEY (event_id)
        REFERENCES curly_store.processed_events(event_id),

    CONSTRAINT CK_orders_quantity CHECK (quantity > 0),
    CONSTRAINT CK_orders_status CHECK (
        status IN ('completed', 'partially_refunded', 'refunded')
    )
);

CREATE TABLE curly_store.refunds (
    refund_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    event_id UNIQUEIDENTIFIER NOT NULL UNIQUE,
    order_id UNIQUEIDENTIFIER NOT NULL,

    refund_amount DECIMAL(19, 2) NOT NULL,
    refunded_at DATETIME2(3) NOT NULL,
    reason NVARCHAR(500) NULL,

    CONSTRAINT FK_refunds_event FOREIGN KEY (event_id)
        REFERENCES curly_store.processed_events(event_id),

    CONSTRAINT FK_refunds_order FOREIGN KEY (order_id)
        REFERENCES curly_store.orders(order_id),

    CONSTRAINT CK_refunds_amount CHECK (refund_amount > 0)
);
