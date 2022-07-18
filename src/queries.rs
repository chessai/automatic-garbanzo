use crate::types::*;

use std::str::FromStr;

// We just care about Sqlite, so this type synonym is used everywhere
pub type Pool = sqlx::Pool<sqlx::Sqlite>;

#[derive(Debug)]
// The errors could be richer (contain more information), but there's no need for that.
// Here, at most we'll print them to stderr.
// They're still very useful for testing/debugging.
pub enum Error {
    // There was an error when interacting with SQLite.
    Sqlx(sqlx::Error),
    // A Tx references a TxID which doesn't exist.
    TxDoesntExist(TxID),
    // There was an attempt to create Tx with a TxID that already exists.
    TxAlreadyExists(TxID),
    // There was a reference to an Account which does not exist.
    //
    // N.B. Accounts must have been created by a Deposit before any other references.
    AccountDoesntExist(AccountID),
    // A Tx would subtract more from an Account's available funds than what the Account has.
    InsufficientFunds(TxID),
    // A resolve or chargeback would subtract more from an Account's held funds than what the Account has.
    InsufficientHeldFunds(AccountID),
    // There was an attempt to operate on a locked account.
    AccountLocked(AccountID),
    // There was an attempt to dispute a Tx that is already in dispute.
    TxAlreadyInDispute(TxID),
    // There was an attempt to resolve or chargeback a Tx that was not already in dispute.
    TxNotInDispute(TxID),
    // There was an attempt to dispute an already-disputed Tx.
    TxNotDisputable(TxID),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlx(sqlx_err) => sqlx_err.fmt(f),
            Self::TxDoesntExist(tx_id) => {
                write!(f, "Tx {:?} is referenced, but doesn't exist", tx_id)
            }
            Self::TxAlreadyExists(tx_id) => {
                write!(f, "There was an attempt to create a Tx with TxID {:?}, but a Tx with that TxID already exists", tx_id)
            }
            Self::AccountDoesntExist(account_id) => {
                write!(
                    f,
                    "There was a reference to a Account ({:?}) that does not exist",
                    account_id
                )
            }
            Self::InsufficientFunds(tx_id) => {
                write!(f, "Tx {:?} would subtract more from a Account's available funds than what the Account has", tx_id)
            }
            Self::InsufficientHeldFunds(account_id) => {
                write!(f, "A resolve or chargeback would subtract more from an Account's ({:?}) held funds than what the Account has", account_id)
            }
            Self::AccountLocked(account_id) => {
                write!(
                    f,
                    "There was an attempt to operate on a Account ({:?}) which is locked",
                    account_id
                )
            }
            Self::TxAlreadyInDispute(tx_id) => {
                write!(
                    f,
                    "There was an attempt to dispute a Tx ({:?}) that is already in dispute",
                    tx_id
                )
            }
            Self::TxNotInDispute(tx_id) => {
                write!(f, "There was an attempt to resolve or chargeback a Tx ({:?}) that was not already in dispute", tx_id)
            }
            Self::TxNotDisputable(tx_id) => {
                write!(
                    f,
                    "There was an attempt to dispute a Tx ({:?}) which is not disputable",
                    tx_id
                )
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Self::Sqlx(err)
    }
}

impl From<sqlx::migrate::MigrateError> for Error {
    fn from(err: sqlx::migrate::MigrateError) -> Self {
        Self::Sqlx(sqlx::Error::from(err))
    }
}

fn assert_tx_not_in_dispute(tx: &Tx) -> Result<(), Error> {
    if tx.in_dispute() {
        return Err(Error::TxAlreadyInDispute(tx.id()));
    }

    Ok(())
}

fn assert_tx_in_dispute(tx: &Tx) -> Result<(), Error> {
    if !tx.in_dispute() {
        return Err(Error::TxNotInDispute(tx.id()));
    }

    Ok(())
}

fn assert_tx_disputable(tx: &Tx) -> Result<(), Error> {
    if !tx.disputable() {
        return Err(Error::TxNotDisputable(tx.id()));
    }

    Ok(())
}

fn assert_account_not_locked(account: &Account) -> Result<(), Error> {
    if account.locked {
        return Err(Error::AccountLocked(account.id));
    }

    Ok(())
}

async fn assert_tx_doesnt_exist(tx_id: &TxID, pool: &Pool) -> Result<(), Error> {
    if fetch_tx_optional(tx_id, pool).await?.is_some() {
        return Err(Error::TxAlreadyExists(*tx_id));
    }

    Ok(())
}

// Record (insert) a transaction into the transactions table.
async fn record_tx(tx: &Tx, pool: &Pool) -> Result<(), sqlx::Error> {
    let (account_id, id, amount) = match tx {
        Tx::Deposit {
            account_id,
            id,
            amount,
            ..
        } => (account_id, id, amount),
        Tx::Withdrawal {
            account_id,
            id,
            amount,
            ..
        } => (account_id, id, amount),
    };
    let dispute_status = DisputeStatus::NotInDispute;

    sqlx::query("INSERT INTO transactions (type, account_id, id, amount, dispute_status) VALUES (?, ?, ?, ?, ?)")
        .bind(tx.type_string())
        .bind(account_id)
        .bind(id)
        .bind(amount)
        .bind(&dispute_status)
        .execute(pool)
        .await?;

    Ok(())
}

// Open an account with some initial funds.
async fn open_account(
    account_id: &AccountID,
    initial_funds: &Amount,
    pool: &Pool,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO accounts (id, available, held, locked) VALUES (?, ?, ?, ?)")
        .bind(account_id)
        .bind(initial_funds)
        .bind(&Amount::zero())
        .bind(false)
        .execute(pool)
        .await?;

    Ok(())
}

async fn fetch_tx_optional(tx_id: &TxID, pool: &Pool) -> Result<Option<Tx>, sqlx::Error> {
    sqlx::query_as::<_, Tx>("SELECT * FROM transactions WHERE id = ?")
        .bind(tx_id)
        .fetch_optional(pool)
        .await
}

async fn fetch_tx(tx_id: &TxID, pool: &Pool) -> Result<Tx, Error> {
    let tx = fetch_tx_optional(tx_id, pool)
        .await?
        .ok_or(Error::TxDoesntExist(*tx_id))?;

    Ok(tx)
}

async fn fetch_account_optional(
    account_id: &AccountID,
    pool: &Pool,
) -> Result<Option<Account>, sqlx::Error> {
    sqlx::query_as::<_, Account>("SELECT * FROM accounts WHERE id = ?")
        .bind(account_id)
        .fetch_optional(pool)
        .await
}

async fn fetch_account(account_id: &AccountID, pool: &Pool) -> Result<Account, Error> {
    let account = fetch_account_optional(account_id, pool)
        .await?
        .ok_or(Error::AccountDoesntExist(*account_id))?;

    Ok(account)
}

// Mark a transaction as in_dispute.
async fn mark_in_dispute(tx_id: &TxID, pool: &Pool) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE transactions SET dispute_status = ? WHERE id = ?")
        .bind(&DisputeStatus::InDispute)
        .bind(tx_id)
        .execute(pool)
        .await?;

    Ok(())
}

async fn mark_not_in_dispute(tx_id: &TxID, pool: &Pool) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE transactions SET dispute_status = ? WHERE id = ?")
        .bind(&DisputeStatus::NotInDispute)
        .bind(tx_id)
        .execute(pool)
        .await?;

    Ok(())
}

async fn mark_not_disputable(tx_id: &TxID, pool: &Pool) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE transactions SET dispute_status = ? WHERE id = ?")
        .bind(&DisputeStatus::NotDisputable)
        .bind(tx_id)
        .execute(pool)
        .await?;

    Ok(())
}

async fn lock_account(account_id: &AccountID, pool: &Pool) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE accounts SET locked = ? WHERE id = ?")
        .bind(true)
        .bind(account_id)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn run_input(input: &Input, pool: &Pool) -> Result<(), Error> {
    match input {
        Input::Tx(tx) => run_tx(tx, pool).await,
        Input::Dispute(dispute) => run_dispute(dispute, pool).await,
    }
}

async fn run_tx(tx: &Tx, pool: &Pool) -> Result<(), Error> {
    match tx {
        // this is an upsert, but a bit more complicated, because we want to do the arithmetic
        // on the rust side in order to retain correctness. we can't use a regular SQL upsert
        // because while we can inject values into SQL (with .bind()), we can't extract values
        // out (we would need accounts.available)
        Tx::Deposit {
            account_id,
            id,
            amount,
            ..
        } => {
            assert_tx_doesnt_exist(id, pool).await?;

            match fetch_account_optional(account_id, pool).await? {
                // The account doesn't need creating, so we just add to the available funds
                Some(account) => {
                    assert_account_not_locked(&account)?;

                    let new_amount = &account.available + amount;
                    sqlx::query("UPDATE accounts SET available = ? WHERE id = ?")
                        .bind(&new_amount)
                        .bind(account_id)
                        .execute(pool)
                        .await?;
                }
                // The account doesn't exist, so we create it, with its initial available funds
                // set to the amount in the tx
                None => {
                    open_account(account_id, amount, pool).await?;
                }
            }
        }
        Tx::Withdrawal {
            account_id,
            id,
            amount,
            ..
        } => {
            assert_tx_doesnt_exist(id, pool).await?;

            let account = fetch_account(account_id, pool).await?;
            assert_account_not_locked(&account)?;

            let new_available =
                (&account.available - amount).ok_or(Error::InsufficientFunds(*id))?;

            sqlx::query("UPDATE accounts SET available = ? WHERE id = ?")
                .bind(&new_available)
                .bind(account_id)
                .execute(pool)
                .await?;
        }
    }

    record_tx(tx, pool).await?;

    Ok(())
}

async fn run_dispute(dispute: &Dispute, pool: &Pool) -> Result<(), Error> {
    match dispute {
        Dispute::Claim { account_id, tx_id } => {
            let disputed_tx = fetch_tx(tx_id, pool).await?;
            assert_tx_not_in_dispute(&disputed_tx)?;
            assert_tx_disputable(&disputed_tx)?;

            let account = fetch_account(account_id, pool).await?;
            assert_account_not_locked(&account)?;

            let new_available = (&account.available - disputed_tx.amount())
                .ok_or(Error::InsufficientFunds(*tx_id))?;
            let new_held = &account.held + disputed_tx.amount();

            sqlx::query("UPDATE accounts SET available = ?, held = ? WHERE id = ?")
                .bind(new_available)
                .bind(new_held)
                .bind(account_id)
                .execute(pool)
                .await?;
            mark_in_dispute(tx_id, pool).await?;
        }
        Dispute::Resolve { account_id, tx_id } => {
            let disputed_tx = fetch_tx(tx_id, pool).await?;
            assert_tx_in_dispute(&disputed_tx)?;

            let account = fetch_account(account_id, pool).await?;
            assert_account_not_locked(&account)?;

            let new_available = &account.available + disputed_tx.amount();
            let new_held = (&account.held - disputed_tx.amount())
                .ok_or(Error::InsufficientHeldFunds(*account_id))?;

            sqlx::query("UPDATE accounts SET available = ?, held = ? WHERE id = ?")
                .bind(new_available)
                .bind(new_held)
                .bind(account_id)
                .execute(pool)
                .await?;
            mark_not_in_dispute(tx_id, pool).await?;
        }
        Dispute::Chargeback { account_id, tx_id } => {
            let disputed_tx = fetch_tx(tx_id, pool).await?;
            assert_tx_in_dispute(&disputed_tx)?;

            // Should a locked account be able to have additional
            // chargebacks performed against it? Locking an account
            // against which there are multiple disputes should
            // probably not prevent further disputes from resolution
            // against the same account. For simplicity, this is not
            // the case in the code as of right now. Filing unlimited
            // disputes against an account is a DOS vector, but it is
            // not clear if this should be something defended against
            // here or elsewhere.
            let account = fetch_account(account_id, pool).await?;
            assert_account_not_locked(&account)?;

            // Lock the account before anything else, in case any subsequent queries fail
            lock_account(account_id, pool).await?;
            // Set the tx as not disputable, since the funds have been returned
            mark_not_disputable(tx_id, pool).await?;

            let new_held = (&account.held - disputed_tx.amount())
                .ok_or(Error::InsufficientHeldFunds(*account_id))?;

            sqlx::query("UPDATE accounts SET held = ? WHERE id = ?")
                .bind(new_held)
                .bind(account_id)
                .execute(pool)
                .await?;
        }
    }

    Ok(())
}

pub async fn db_setup() -> Result<Pool, sqlx::Error> {
    use sqlx::sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
    };

    let timeout = std::time::Duration::from_secs(30);

    let connection_options = SqliteConnectOptions::from_str("sqlite::memory:")?
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(timeout);

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_timeout(timeout)
        .connect_with(connection_options)
        .await?;

    sqlx::query("pragma temp_store = memory;")
        .execute(&pool)
        .await?;
    sqlx::query("pragma mmap_size = 30000000000;")
        .execute(&pool)
        .await?;
    sqlx::query("pragma page_size = 4096;")
        .execute(&pool)
        .await?;

    sqlx::migrate!("./db").run(&pool).await?;

    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Generate a TxID different than the current one, for testing
    // purposes.
    //
    // Ideally we would want a fresh TxID, produced via a Factory.
    // This gets us at least as far as avoiding overflow/underflow.
    fn different_tx_id(tx_id: TxID) -> TxID {
        let u = tx_id.0;
        TxID(u.checked_add(1).or_else(|| u.checked_sub(1)).unwrap())
    }

    // return an ordered pair of the arguments, where the lesser comes first
    fn ordered_pair<T>(fst: T, snd: T) -> (T, T)
    where
        T: Ord,
    {
        use std::cmp::Ordering;

        match fst.cmp(&snd) {
            Ordering::Less => (fst, snd),
            Ordering::Equal => (fst, snd),
            Ordering::Greater => (snd, fst),
        }
    }

    // Fetch the number of transactions in the database.
    //
    // This is used to verify the number of transactions before
    // and after each test. Right now this is manual, but it would
    // be better to write a function that wraps each test to do that.
    // We avoided this for simplicity.
    async fn fetch_num_txs(pool: &Pool) -> Result<u32, Error> {
        let num_txs = sqlx::query_scalar("SELECT COUNT(*) FROM transactions")
            .fetch_one(pool)
            .await?;

        Ok(num_txs)
    }

    // Test a deposit, followed by a withdrawal, against the same (new) account.
    // - Generate two amounts, amt1 and amt2
    // - (deposit, withdrawal) = (max amt1 amt2, min amt1 amnt2)
    // - perform deposit, then perform withdrawal
    // - Verify that account table state is as we expect (single account with funds = deposit - withdrawal)
    // - Verify that the transaction table state is as we expect (two transactions equivalent to what we submitted)
    #[quickcheck_async::tokio]
    async fn prop_simple_deposit_withdrawal(
        account_id: AccountID,
        tx_id_deposit: TxID,
        amount1: Amount,
        amount2: Amount,
    ) -> Result<(), Error> {
        let pool = db_setup().await?;

        // check that there are no transactions
        assert_eq!(fetch_num_txs(&pool).await?, 0);

        let (withdrawal_amount, deposit_amount) = ordered_pair(amount1, amount2);
        let expected_resulting_amount = (&deposit_amount - &withdrawal_amount).unwrap();

        let txs = &[
            Tx::new_deposit(account_id, tx_id_deposit, deposit_amount),
            Tx::new_withdrawal(
                account_id,
                different_tx_id(tx_id_deposit),
                withdrawal_amount,
            ),
        ];

        // run transactions
        for tx in txs {
            run_tx(tx, &pool).await?;
        }

        // check that the accounts table state is what we expect it to be
        {
            let expected_accounts = vec![Account::open_with(account_id, expected_resulting_amount)];

            let actual_accounts =
                sqlx::query_as::<_, Account>("SELECT * FROM accounts ORDER BY id")
                    .fetch_all(&pool)
                    .await?;

            assert_eq!(expected_accounts, actual_accounts);
        }

        // check that the transactions table state is what we expect it to be
        {
            let expected_transactions = {
                let mut_txs = &mut txs.clone();
                mut_txs.sort_by_key(|tx| tx.id());
                mut_txs.to_vec()
            };

            let actual_transactions =
                sqlx::query_as::<_, Tx>("SELECT * FROM transactions ORDER BY id")
                    .fetch_all(&pool)
                    .await?;

            assert_eq!(expected_transactions, actual_transactions);
        }

        // check that the number rows in the transactions table is what we expect it to be
        assert_eq!(fetch_num_txs(&pool).await?, txs.len() as u32);

        Ok(())
    }

    // Tests that submitting a transaction with the same TxID as
    // one before it is disallowed.
    #[quickcheck_async::tokio]
    async fn prop_cant_submit_tx_that_already_exists(
        account_id: AccountID,
        tx_id: TxID,
        amount: Amount,
    ) -> Result<(), Error> {
        let pool = db_setup().await?;

        // check that there are no transactions
        assert_eq!(fetch_num_txs(&pool).await?, 0);

        let original_tx = Tx::new_deposit(account_id, tx_id, amount);
        run_tx(&original_tx, &pool).await?;

        let bad_tx = original_tx.clone();
        match run_tx(&bad_tx, &pool).await {
            Err(Error::TxAlreadyExists(_)) => Ok(()) as Result<(), Error>,
            Err(err) => panic!("transaction returned an unexpected error: {:?}", err),
            Ok(_) => panic!("bad transaction was accepted!"),
        }?;

        // check that there is exactly one transaction
        assert_eq!(fetch_num_txs(&pool).await?, 1);

        Ok(())
    }

    // Test that you cannot withdraw more from an account
    // than exists in its available funds.
    #[quickcheck_async::tokio]
    async fn prop_cant_overdraft(
        account_id: AccountID,
        tx_id: TxID,
        amount: Amount,
    ) -> Result<(), Error> {
        let pool = db_setup().await?;

        // check that there are no transactions
        assert_eq!(fetch_num_txs(&pool).await?, 0);

        let tx = Tx::new_deposit(account_id, tx_id, amount.clone());
        run_tx(&tx, &pool).await?;

        let account_before = fetch_account(&account_id, &pool).await?;

        let bad_tx =
            Tx::new_withdrawal(account_id, different_tx_id(tx_id), &amount + &Amount::one());

        match run_tx(&bad_tx, &pool).await {
            Err(Error::InsufficientFunds(_)) => (),
            Err(err) => panic!("transaction returned an unexpected error: {:?}", err),
            Ok(_) => panic!("bad transaction was accepted!"),
        };

        let account_after = fetch_account(&account_id, &pool).await?;

        // check that the funds have not changed since the deposit (i.e. withdrawal should not go through)
        assert_eq!(account_before, account_after);

        // check that there is exactly one transaction
        assert_eq!(fetch_num_txs(&pool).await?, 1);

        Ok(())
    }

    // A simple property test to test the following timeline:
    // - Submit a transaction
    // - Dispute the transaction
    // - Resolve the dispute
    // And that all resulting state is as expected.
    #[quickcheck_async::tokio]
    async fn prop_simple_dispute_resolve(
        account_id: AccountID,
        tx_id: TxID,
        amount: Amount,
    ) -> Result<(), Error> {
        let pool = db_setup().await?;

        // check that there are no transactions
        assert_eq!(fetch_num_txs(&pool).await?, 0);

        run_tx(&Tx::new_deposit(account_id, tx_id, amount.clone()), &pool).await?;
        run_dispute(&Dispute::Claim { account_id, tx_id }, &pool).await?;

        // check that the tx was marked as in dispute
        assert!(fetch_tx(&tx_id, &pool).await?.in_dispute());
        // check that the amount in the tx has been held in the account
        assert!(fetch_account(&account_id, &pool).await?.held == amount);

        run_dispute(&Dispute::Resolve { account_id, tx_id }, &pool).await?;

        // after the resolution, check that the tx was marked as no longer in dispute
        assert!(!fetch_tx(&tx_id, &pool).await?.in_dispute());

        {
            let expected_account = Account::open_with(account_id, amount);

            let actual_account = fetch_account(&account_id, &pool).await?;

            assert_eq!(expected_account, actual_account);
        }

        // check that there is exactly one transaction
        assert_eq!(fetch_num_txs(&pool).await?, 1);

        Ok(())
    }

    // A simple property test to test the following timeline:
    // - Submit a transaction
    // - Dispute the transaction
    // - Chargeback the dispute
    // And that all resulting state is as expected.
    #[quickcheck_async::tokio]
    async fn prop_simple_dispute_chargeback(
        account_id: AccountID,
        tx_id: TxID,
        amount: Amount,
    ) -> Result<(), Error> {
        let pool = db_setup().await?;

        // check that there are no transactions
        assert_eq!(fetch_num_txs(&pool).await?, 0);

        run_tx(&Tx::new_deposit(account_id, tx_id, amount.clone()), &pool).await?;
        run_dispute(&Dispute::Claim { account_id, tx_id }, &pool).await?;

        // check that the tx was marked as in dispute
        assert!(fetch_tx(&tx_id, &pool).await?.in_dispute());
        // check that the amount in the tx has been held in the account
        assert!(fetch_account(&account_id, &pool).await?.held == amount);

        run_dispute(&Dispute::Chargeback { account_id, tx_id }, &pool).await?;

        // after the resolution, check that the tx was marked as no longer disputable
        assert!(!fetch_tx(&tx_id, &pool).await?.disputable());

        // check that the account has the expected funds and is locked
        {
            let expected_account = Account::new(account_id, Amount::zero(), Amount::zero(), true);
            let actual_account = fetch_account(&account_id, &pool).await?;

            assert_eq!(expected_account, actual_account);
        }

        // check that there is exactly one transaction
        assert_eq!(fetch_num_txs(&pool).await?, 1);

        Ok(())
    }

    // A simple test to show that a transaction that has been
    // Chargebacked cannot be disputed.
    #[quickcheck_async::tokio]
    async fn prop_cant_dispute_chargeback_tx(
        account_id: AccountID,
        tx_id: TxID,
        amount: Amount,
    ) -> Result<(), Error> {
        let pool = db_setup().await?;

        // check that there are no transactions
        assert_eq!(fetch_num_txs(&pool).await?, 0);

        // Create the transaction
        run_tx(&Tx::new_deposit(account_id, tx_id, amount.clone()), &pool).await?;
        // Dispute the transaction
        run_dispute(&Dispute::Claim { account_id, tx_id }, &pool).await?;
        // Chargeback the transaction
        run_dispute(&Dispute::Chargeback { account_id, tx_id }, &pool).await?;

        // Attempt to chargeback again
        match run_dispute(&Dispute::Claim { account_id, tx_id }, &pool).await {
            Err(Error::TxNotDisputable(_)) => Ok(()) as Result<(), Error>,
            Err(err) => panic!("Disputing a non-disputable transaction resulted in an unexpected error: {:?}", err),
            Ok(_) => panic!("Disputing a non-disputable transaction resulted in no error, while one was expected"),
        }?;

        // check that there is exactly one transaction
        assert_eq!(fetch_num_txs(&pool).await?, 1);

        Ok(())
    }
}
