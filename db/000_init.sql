CREATE TABLE IF NOT EXISTS accounts (
    "id" INTEGER NOT NULL,
    "available" TEXT NOT NULL,
    "held" TEXT NOT NULL,
    "locked" INTEGER NOT NULL,

    PRIMARY KEY("id")
);
CREATE UNIQUE INDEX idx_accounts_on_id ON accounts(id);

CREATE TABLE IF NOT EXISTS transactions (
    "type" TEXT NOT NULL,
    "account_id" INTEGER NOT NULL,
    "id" INTEGER NOT NULL,
    "amount" TEXT NOT NULL,
    "dispute_status" TEXT NOT NULL,

    PRIMARY KEY("id"),
    FOREIGN KEY("account_id") REFERENCES accounts("id")
);
CREATE UNIQUE INDEX idx_transactions_on_id ON transactions(id);
