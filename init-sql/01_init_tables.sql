CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id bigint primary key,
    client_id bigint,
    amount decimal(15, 2),
    category varchar(100),
    transaction_date timestamp with time zone
);

CREATE TABLE IF NOT EXISTS dm_client_activity (
    client_id bigint,
    activity_date date,
    total_amount decimal(15, 2),
    transaction_count int,
    primary key (client_id, activity_date)
);