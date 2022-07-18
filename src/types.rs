use quickcheck::{Arbitrary, Gen};
use rust_decimal::Decimal;
use rusty_money::{iso, Money, MoneyError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::str::FromStr;

#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sqlx::Type,
)]
#[sqlx(transparent)]
pub struct AccountID(pub u16);

impl Arbitrary for AccountID {
    fn arbitrary(g: &mut Gen) -> Self {
        AccountID(u16::arbitrary(g))
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(AccountID))
    }
}

#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sqlx::Type,
)]
#[sqlx(transparent)]
pub struct TxID(pub u32);

impl Arbitrary for TxID {
    fn arbitrary(g: &mut Gen) -> Self {
        TxID(u32::arbitrary(g))
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(TxID))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Amount(Money<'static, iso::Currency>); // I believe the 'static lifetime is okay, because it just refers to the choice of currency (see Amount::CURRENCY)

impl Amount {
    // Our data is technically without currency, but we
    // have to pick something. In the real world there would
    // be multiple currencies; and the Money type allows us
    // to branch out into handling that case, along with
    // conversions, all in a fairly lightweight way.
    const CURRENCY: &'static iso::Currency = iso::USD;

    // Scale for construction via Decimal. See
    // https://docs.rs/rust_decimal/latest/rust_decimal/prelude/struct.Decimal.html#method.new
    //
    // Used in the Arbitrary instance alone for now
    const SCALE: u32 = 4;

    pub fn zero() -> Self {
        Self::from_decimal(Decimal::ZERO)
    }

    pub fn one() -> Self {
        Self::from_decimal(Decimal::ONE)
    }

    pub fn from_decimal(amount: Decimal) -> Self {
        // Should be cheap to clone; no reason to impose a
        // mutability constraint on users. Potentially there
        // could be another version of this function which takes
        // a &mut Decimal, but this is unnecessary for now
        let mut ensured_positive = amount;
        ensured_positive.set_sign_positive(true);
        Amount(Money::from_decimal(ensured_positive, Self::CURRENCY))
    }

    pub fn to_decimal(&self) -> &Decimal {
        self.0.amount()
    }
}

impl<'a, 'b> std::ops::Add<&'b Amount> for &'a Amount {
    type Output = Amount;

    fn add(self, other: &'b Amount) -> Amount {
        Amount(Money::from_decimal(
            self.to_decimal() + other.to_decimal(),
            Amount::CURRENCY,
        ))
    }
}

impl<'a, 'b> std::ops::Sub<&'b Amount> for &'a Amount {
    type Output = Option<Amount>;

    fn sub(self, other: &'b Amount) -> Option<Amount> {
        let x = self.to_decimal() - other.to_decimal();
        if x < Decimal::ZERO {
            None
        } else {
            Some(Amount(Money::from_decimal(x, Amount::CURRENCY)))
        }
    }
}

impl ToString for Amount {
    fn to_string(&self) -> String {
        self.0.amount().to_string()
    }
}

impl FromStr for Amount {
    // This should probably have a dedicated error type,
    // because MoneyError::InvalidAmount doesn't really
    // mean that the amount was negative, which is how
    // we're using it here.
    type Err = MoneyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let money = Money::from_str(s, Self::CURRENCY)?;
        if money.amount() < &Decimal::ZERO {
            return Err(MoneyError::InvalidAmount);
        }
        Ok(Amount(money))
    }
}

impl Serialize for Amount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.amount().to_string())
    }
}

impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        Amount::from_str(&buf).map_err(serde::de::Error::custom)
    }
}

impl sqlx::Type<sqlx::Sqlite> for Amount {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        <str as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Sqlite> for Amount {
    fn encode(self, args: &mut Vec<sqlx::sqlite::SqliteArgumentValue<'q>>) -> sqlx::encode::IsNull {
        sqlx::Encode::encode(self.to_string(), args)
    }

    fn encode_by_ref(
        &self,
        args: &mut Vec<sqlx::sqlite::SqliteArgumentValue<'q>>,
    ) -> sqlx::encode::IsNull {
        sqlx::Encode::encode_by_ref(&self.to_string(), args)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Sqlite> for Amount {
    // TODO: this is unsafe. Fit MoneyError into BoxDynError
    fn decode(value: sqlx::sqlite::SqliteValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        Ok(Self::from_str(sqlx::Decode::decode(value)?).unwrap())
    }
}

impl Arbitrary for Amount {
    fn arbitrary(g: &mut Gen) -> Self {
        let num = i64::from(u32::arbitrary(g));
        Amount::from_decimal(Decimal::new(num, Amount::SCALE))
    }
}

// The dispute status of a transaction.
// This helps prevent double-dispute for transactions currently in
// dispute and those already disputed.
//
// It's possible that this type could be richer (e.g. `NotDisputable`
// could inform why), or Disputes could be stored and refer to the
// transactions table.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
#[derive(sqlx::Type)]
#[sqlx(rename_all = "snake_case")]
pub enum DisputeStatus {
    // A transaction that is not in dispute. These transactions
    // can be disputed.
    NotInDispute,
    // A transaction that is in dispute (a `Claim` has been filed
    // against it). There has been no resolution to the dispute
    // via `Resolve` or `Chargeback`.
    InDispute,
    // A transaction is not disputable, because it has been
    // `Chargeback`ed. We do not prevent previously-disputed
    // but `Resolve`d transactions from being disputed again,
    // i.e. a `Resolve` will change the DisputeStatus to
    // `NotInDispute`.
    NotDisputable,
}

impl Arbitrary for DisputeStatus {
    fn arbitrary(g: &mut Gen) -> Self {
        g.choose(&[Self::NotInDispute, Self::InDispute, Self::NotDisputable])
            .cloned()
            .unwrap()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Input {
    Tx(Tx),
    Dispute(Dispute),
}

impl Serialize for Input {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        input_serde::serialize_input(self, serializer)
    }
}

impl<'de> Deserialize<'de> for Input {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        input_serde::deserialize_input(deserializer)
    }
}

impl Arbitrary for Input {
    fn arbitrary(g: &mut Gen) -> Self {
        input_serde::arbitrary_input(g)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Tx {
    Deposit {
        account_id: AccountID,
        id: TxID,
        amount: Amount,
        dispute_status: DisputeStatus,
    },
    Withdrawal {
        account_id: AccountID,
        id: TxID,
        amount: Amount,
        dispute_status: DisputeStatus,
    },
}

impl Arbitrary for Tx {
    fn arbitrary(g: &mut Gen) -> Self {
        input_serde::arbitrary_tx(g)
    }
}

impl Tx {
    #[allow(dead_code)] // Useful in tests.
    pub fn new_deposit(account_id: AccountID, tx_id: TxID, amount: Amount) -> Self {
        Self::Deposit {
            account_id,
            id: tx_id,
            amount,
            dispute_status: DisputeStatus::NotInDispute,
        }
    }

    #[allow(dead_code)] // Useful in tests.
    pub fn new_withdrawal(account_id: AccountID, tx_id: TxID, amount: Amount) -> Self {
        Self::Withdrawal {
            account_id,
            id: tx_id,
            amount,
            dispute_status: DisputeStatus::NotInDispute,
        }
    }

    pub fn type_string(&self) -> String {
        String::from(match self {
            Self::Deposit { .. } => "deposit",
            Self::Withdrawal { .. } => "withdrawal",
        })
    }

    pub fn id(&self) -> TxID {
        match self {
            Self::Deposit { id, .. } => *id,
            Self::Withdrawal { id, .. } => *id,
        }
    }

    pub fn amount(&self) -> &Amount {
        match self {
            Self::Deposit { amount, .. } => amount,
            Self::Withdrawal { amount, .. } => amount,
        }
    }

    pub fn in_dispute(&self) -> bool {
        match self {
            Self::Deposit { dispute_status, .. } => dispute_status == &DisputeStatus::InDispute,
            Self::Withdrawal { dispute_status, .. } => dispute_status == &DisputeStatus::InDispute,
        }
    }

    pub fn disputable(&self) -> bool {
        match self {
            Self::Deposit { dispute_status, .. } => dispute_status == &DisputeStatus::NotInDispute,
            Self::Withdrawal { dispute_status, .. } => {
                dispute_status == &DisputeStatus::NotInDispute
            }
        }
    }
}

// The situation for enums + sqlx is even worse than with enums + csv + serde, in that deriving
// doesn't even work for non-C enums.
// Luckily, the boilerplate here is small, and there's not a lot of room for error.
impl<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for Tx {
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let tx_type = row.try_get("type")?;
        match tx_type {
            "deposit" => Ok(Self::Deposit {
                account_id: row.try_get("account_id")?,
                id: row.try_get("id")?,
                amount: row.try_get("amount")?,
                dispute_status: row.try_get("dispute_status")?,
            }),
            "withdrawal" => Ok(Self::Withdrawal {
                account_id: row.try_get("account_id")?,
                id: row.try_get("id")?,
                amount: row.try_get("amount")?,
                dispute_status: row.try_get("dispute_status")?,
            }),
            unknown_type => Err(sqlx::Error::Decode(Box::new(InvalidDiscriminant(
                String::from(unknown_type),
            )))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Dispute {
    Claim { account_id: AccountID, tx_id: TxID },
    Resolve { account_id: AccountID, tx_id: TxID },
    Chargeback { account_id: AccountID, tx_id: TxID },
}

// The situation for enums + sqlx is even worse than with enums + csv + serde.
// Luckily, the boilerplate here is small, and there's not a lot of room for error.
impl<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for Dispute {
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let dispute_type = row.try_get("type")?;
        match dispute_type {
            "dispute" => Ok(Self::Claim {
                account_id: row.try_get("account_id")?,
                tx_id: row.try_get("tx_id")?,
            }),
            "resolve" => Ok(Self::Resolve {
                account_id: row.try_get("account_id")?,
                tx_id: row.try_get("tx_id")?,
            }),
            "chargeback" => Ok(Self::Chargeback {
                account_id: row.try_get("account_id")?,
                tx_id: row.try_get("tx_id")?,
            }),
            unknown_type => Err(sqlx::Error::Decode(Box::new(InvalidDiscriminant(
                String::from(unknown_type),
            )))),
        }
    }
}

// Error when the type discriminant is invalid during sqlx::FromRow decoding.
#[derive(Debug)]
struct InvalidDiscriminant(String);

impl std::fmt::Display for InvalidDiscriminant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Encountered unknown discriminant when performing sqlx::FromRow decoding: {}",
            self.0
        )
    }
}

impl std::error::Error for InvalidDiscriminant {}

mod input_serde {
    use crate::types::{AccountID, Amount, Dispute, DisputeStatus, Input, Tx, TxID};
    use quickcheck::Arbitrary;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    #[serde(rename_all = "lowercase")]
    enum InputType {
        Deposit,
        Withdrawal,
        Dispute,
        Resolve,
        Chargeback,
    }

    impl ToString for InputType {
        fn to_string(&self) -> String {
            String::from(match self {
                Self::Deposit => "deposit",
                Self::Withdrawal => "withdrawal",
                Self::Dispute => "dispute",
                Self::Resolve => "resolve",
                Self::Chargeback => "chargeback",
            })
        }
    }

    impl quickcheck::Arbitrary for InputType {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            g.choose(&[
                Self::Deposit,
                Self::Withdrawal,
                Self::Dispute,
                Self::Resolve,
                Self::Chargeback,
            ])
            .cloned()
            .unwrap()
        }
    }

    // A wrapper for Input's serde. See https://github.com/BurntSushi/rust-csv/issues/211
    // for why a safer-by-construction representation would not work with csv + serde.
    //
    // Another approach would be to manually write the serde implementations for Input,
    // but that would be harder to read, write, and maintain, and give us less support
    // from the compiler. It would be marginally faster, but certainly not enough to
    // offset all the downsides.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct InputSerde {
        r#type: InputType,
        client: AccountID,
        tx: TxID,
        amount: Option<Amount>,
    }

    impl quickcheck::Arbitrary for InputSerde {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let input_type = InputType::arbitrary(g);
            let amount = match input_type {
                InputType::Deposit => Some(Amount::arbitrary(g)),
                InputType::Withdrawal => Some(Amount::arbitrary(g)),
                _ => None,
            };

            Self {
                r#type: input_type,
                client: AccountID::arbitrary(g),
                tx: TxID::arbitrary(g),
                amount,
            }
        }
    }

    // Error when calling Input::deserialize()
    #[derive(Debug)]
    enum InputDeserializeError {
        MissingAmount(InputType),
    }

    impl std::fmt::Display for InputDeserializeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::MissingAmount(input_type) => {
                    write!(f, "{} is missing an amount", input_type.to_string())
                }
            }
        }
    }

    impl std::error::Error for InputDeserializeError {}

    pub fn serialize_input<S>(input: &Input, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        InputSerde::serialize(&to_serde_type(input), serializer)
    }

    pub fn deserialize_input<'de, D>(deserializer: D) -> Result<Input, D::Error>
    where
        D: Deserializer<'de>,
    {
        from_serde_type(&InputSerde::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }

    pub fn arbitrary_input(g: &mut quickcheck::Gen) -> Input {
        from_serde_type(&InputSerde::arbitrary(g)).unwrap()
    }

    pub fn arbitrary_tx(g: &mut quickcheck::Gen) -> Tx {
        let input_type = g
            .choose(&[InputType::Deposit, InputType::Withdrawal])
            .cloned()
            .unwrap();
        match input_type {
            InputType::Deposit => Tx::Deposit {
                account_id: AccountID::arbitrary(g),
                id: TxID::arbitrary(g),
                amount: Amount::arbitrary(g),
                dispute_status: DisputeStatus::arbitrary(g),
            },
            InputType::Withdrawal => Tx::Withdrawal {
                account_id: AccountID::arbitrary(g),
                id: TxID::arbitrary(g),
                amount: Amount::arbitrary(g),
                dispute_status: DisputeStatus::arbitrary(g),
            },
            x => panic!("Tx::arbitrary(): encountered unexpected InputType: {:?}", x),
        }
    }

    fn to_serde_type(input: &Input) -> InputSerde {
        match input {
            Input::Tx(tx) => match tx {
                Tx::Deposit {
                    account_id,
                    id,
                    amount,
                    ..
                } => InputSerde {
                    r#type: InputType::Deposit,
                    client: *account_id,
                    tx: *id,
                    amount: Some(amount.clone()),
                },
                Tx::Withdrawal {
                    account_id,
                    id,
                    amount,
                    ..
                } => InputSerde {
                    r#type: InputType::Withdrawal,
                    client: *account_id,
                    tx: *id,
                    amount: Some(amount.clone()),
                },
            },
            Input::Dispute(dispute) => match dispute {
                Dispute::Claim { account_id, tx_id } => InputSerde {
                    r#type: InputType::Dispute,
                    client: *account_id,
                    tx: *tx_id,
                    amount: None,
                },
                Dispute::Resolve { account_id, tx_id } => InputSerde {
                    r#type: InputType::Resolve,
                    client: *account_id,
                    tx: *tx_id,
                    amount: None,
                },
                Dispute::Chargeback { account_id, tx_id } => InputSerde {
                    r#type: InputType::Chargeback,
                    client: *account_id,
                    tx: *tx_id,
                    amount: None,
                },
            },
        }
    }

    fn from_serde_type(input_serde: &InputSerde) -> Result<Input, InputDeserializeError> {
        match input_serde.r#type {
            InputType::Deposit => {
                let amount = input_serde
                    .amount
                    .clone()
                    .ok_or(InputDeserializeError::MissingAmount(InputType::Deposit))?;
                Ok(Input::Tx(Tx::Deposit {
                    account_id: input_serde.client,
                    id: input_serde.tx,
                    amount,
                    dispute_status: DisputeStatus::NotInDispute,
                }))
            }
            InputType::Withdrawal => {
                let amount = input_serde
                    .amount
                    .clone()
                    .ok_or(InputDeserializeError::MissingAmount(InputType::Withdrawal))?;
                Ok(Input::Tx(Tx::Withdrawal {
                    account_id: input_serde.client,
                    id: input_serde.tx,
                    amount,
                    dispute_status: DisputeStatus::NotInDispute,
                }))
            }
            InputType::Dispute => Ok(Input::Dispute(Dispute::Claim {
                account_id: input_serde.client,
                tx_id: input_serde.tx,
            })),
            InputType::Resolve => Ok(Input::Dispute(Dispute::Resolve {
                account_id: input_serde.client,
                tx_id: input_serde.tx,
            })),
            InputType::Chargeback => Ok(Input::Dispute(Dispute::Chargeback {
                account_id: input_serde.client,
                tx_id: input_serde.tx,
            })),
        }
    }
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
// We don't include `total` in `Account`. See the `Serialize` implementation
// of `Account` for an explanation.
pub struct Account {
    pub id: AccountID,
    pub available: Amount,
    pub held: Amount,
    pub locked: bool,
}

impl Account {
    // Used in tests
    #[allow(dead_code)]
    // Create a new, unlocked account with no funds.
    pub fn open(id: AccountID) -> Self {
        Self::new(id, Amount::zero(), Amount::zero(), false)
    }

    // Used in tests
    #[allow(dead_code)]
    // Create a new, unlocked account with some funds.
    pub fn open_with(id: AccountID, initial_funds: Amount) -> Self {
        Self::new(id, initial_funds, Amount::zero(), false)
    }

    // Create a new account with funds (both available and held) and locked status.
    pub fn new(id: AccountID, available: Amount, held: Amount, locked: bool) -> Self {
        Self {
            id,
            available,
            held,
            locked,
        }
    }

    pub fn total(&self) -> Amount {
        &self.available + &self.held
    }
}

// We don't include `total` in `Account`.
//
// If we include the `total` field in the `Account`, and derive serde,
// then we distribute the concern of maintaining the invariant that
// `total` = `available` + `held` to multiple places in the code, which
// means more opportunity for mistakes. This dispersion results in more
// places in code with additional complications, as opposed to one spot
// of a custom serde implementation, whose correctness is easy to test for.
// Additionally, omitting `total` uses less space in SQLite.
//
// Because writing custom Deserialize implementations is somewhat lengthy
// (c.f. https://serde.rs/deserialize-struct.html) and more complicated to
// implement, we choose not to implement it here, as it is not necessary per
// the spec. If we did need the Deserialize implementation, it might be best
// to do something similar to `InputSerde` and route through an intermediary.
impl Serialize for Account {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;

        // number of fields + the additional total field
        let num_fields = 5;
        let mut state = serializer.serialize_struct("Account", num_fields)?;
        state.serialize_field("client", &self.id)?;
        state.serialize_field("available", &self.available)?;
        state.serialize_field("held", &self.held)?;
        state.serialize_field("total", &self.total())?;
        state.serialize_field("locked", &self.locked)?;
        state.end()
    }
}

impl Arbitrary for Account {
    fn arbitrary(g: &mut Gen) -> Self {
        Self::new(
            AccountID::arbitrary(g),
            Amount::arbitrary(g),
            Amount::arbitrary(g),
            bool::arbitrary(g),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use tokio_stream::StreamExt;

    // Helper function to check that csv encoding and decoding roundtrips.
    // Note that this can currently only be used in unit tests, not property tests,
    // because of a [futures bug](https://github.com/rust-lang/futures-rs/issues/2090)
    // that quickcheck_async seems to be affected by.
    async fn csv_roundtrip_async<T>(records: Vec<T>) -> Result<(), csv_async::Error>
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        use std::io::Cursor;

        // +1 for headers
        let capacity = records.len() + 1;

        let mut wtr = csv_async::AsyncWriterBuilder::new()
            .has_headers(true)
            .create_serializer(Cursor::new(Vec::with_capacity(capacity)));
        for record in &records {
            wtr.serialize(record).await?;
        }
        let data = String::from_utf8(wtr.into_inner().await.unwrap().into_inner()).unwrap();

        let mut deserialized_records: Vec<T> = Vec::with_capacity(capacity);
        {
            let mut rdr: csv_async::DeserializeRecordsIntoStream<'_, _, T> =
                csv_async::AsyncReaderBuilder::new()
                    .has_headers(true)
                    .trim(csv_async::Trim::All)
                    .create_deserializer(data.as_bytes()) // Doesn't work with Cursor<Vec<u8>> for some reason
                    .into_deserialize();

            while let Some(r) = rdr.next().await {
                let t = r?;
                deserialized_records.push(t);
            }
        }

        assert_eq!(records, deserialized_records);

        Ok(())
    }

    // This is a unit test because quickcheck_async::tokio runs into this error:
    // https://github.com/rust-lang/futures-rs/issues/2090
    // seemingly only when operating on Cursor or File
    //
    // Test that some inputs roundtrip when encoded then decoded
    #[tokio::test]
    async fn csv_roundtrip_input_async() -> Result<(), csv_async::Error> {
        csv_roundtrip_async(vec![
            Input::Tx(Tx::new_deposit(
                AccountID(1),
                TxID(1),
                Amount::from_str("1.0").unwrap(),
            )),
            Input::Tx(Tx::new_deposit(
                AccountID(2),
                TxID(2),
                Amount::from_str("2.0").unwrap(),
            )),
            Input::Tx(Tx::new_deposit(
                AccountID(1),
                TxID(3),
                Amount::from_str("2.0").unwrap(),
            )),
            Input::Tx(Tx::new_withdrawal(
                AccountID(1),
                TxID(4),
                Amount::from_str("1.5").unwrap(),
            )),
            Input::Tx(Tx::new_withdrawal(
                AccountID(2),
                TxID(5),
                Amount::from_str("3.0").unwrap(),
            )),
            Input::Dispute(Dispute::Claim {
                account_id: AccountID(1),
                tx_id: TxID(1),
            }),
            Input::Dispute(Dispute::Resolve {
                account_id: AccountID(1),
                tx_id: TxID(1),
            }),
            Input::Dispute(Dispute::Chargeback {
                account_id: AccountID(1),
                tx_id: TxID(1),
            }),
        ])
        .await
    }

    // Like `csv_roundtrip_async`, but useful for property tests, because quickcheck_async::tokio fails when operating on Cursor/File.
    fn csv_roundtrip<T>(records: Vec<T>) -> bool
    where
        T: Serialize + DeserializeOwned + PartialEq,
    {
        let mut wtr = csv::WriterBuilder::new().from_writer(vec![]);
        for record in &records {
            wtr.serialize(record).unwrap();
        }
        let data = String::from_utf8(wtr.into_inner().unwrap()).unwrap();

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .trim(csv::Trim::All)
            .from_reader(data.as_bytes());
        let deserialized_records = rdr
            .deserialize()
            .collect::<Result<Vec<T>, csv::Error>>()
            .unwrap();

        records == deserialized_records
    }

    // Like the previous roundtrip functions, but instead of
    // encoding some rust data to a string, then decoding that
    // string and comparing against the original rust data; it
    // starts with the encoded data, decodes into the rust data,
    // and compares against some expected rust data.
    //
    // This is very useful visually since we can see the csv
    // encoding, unlike the other version roundtrip function.
    fn csv_roundtrip_unit<T>(data: String, expected: Vec<T>)
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .trim(csv::Trim::All)
            .from_reader(data.as_bytes());
        let deserialized_records = rdr
            .deserialize()
            .collect::<Result<Vec<T>, csv::Error>>()
            .unwrap();

        assert_eq!(expected, deserialized_records)
    }

    // Simple unit test to check that inputs deserialise in ways
    // we expect.
    #[test]
    fn csv_roundtrip_unit_tx() {
        let data = vec![
            "type, client, tx, amount",
            "deposit, 1, 1, 1.0",
            "deposit, 2, 2, 2.0",
            "deposit, 1, 3, 2.0",
            "withdrawal, 1, 4, 1.5",
            "withdrawal, 2, 5, 3.0",
            "dispute, 1, 1, ",
            "resolve, 1, 1, ",
            "chargeback, 1, 1, ",
        ]
        .join("\n");

        let expected = vec![
            Input::Tx(Tx::new_deposit(AccountID(1), TxID(1), Amount::one())),
            Input::Tx(Tx::new_deposit(
                AccountID(2),
                TxID(2),
                Amount::from_str("2.0").unwrap(),
            )),
            Input::Tx(Tx::new_deposit(
                AccountID(1),
                TxID(3),
                Amount::from_str("2.0").unwrap(),
            )),
            Input::Tx(Tx::new_withdrawal(
                AccountID(1),
                TxID(4),
                Amount::from_str("1.5").unwrap(),
            )),
            Input::Tx(Tx::new_withdrawal(
                AccountID(2),
                TxID(5),
                Amount::from_str("3.0").unwrap(),
            )),
            Input::Dispute(Dispute::Claim {
                account_id: AccountID(1),
                tx_id: TxID(1),
            }),
            Input::Dispute(Dispute::Resolve {
                account_id: AccountID(1),
                tx_id: TxID(1),
            }),
            Input::Dispute(Dispute::Chargeback {
                account_id: AccountID(1),
                tx_id: TxID(1),
            }),
        ];

        csv_roundtrip_unit(data, expected)
    }

    // A test for account encoding. Roundtrip test helpers are not
    // used because we didn't write a Deserialize impl for Account.
    // See `Account` docs for why that is.
    #[test]
    fn csv_encode_unit_account() {
        let account = Account::new(
            AccountID(0),
            Amount::from_str("100.00").unwrap(),
            Amount::from_str("250.00").unwrap(),
            true,
        );
        let mut wtr = csv::WriterBuilder::new().from_writer(vec![]);
        wtr.serialize(account).unwrap();

        // Equate on lines to avoid any concern over trailing newlines
        let actual_data: Vec<String> = String::from_utf8(wtr.into_inner().unwrap())
            .unwrap()
            .lines()
            .map(|x| x.to_string())
            .collect();

        let expected_data = vec![
            "client,available,held,total,locked",
            "0,100.00,250.00,350.00,true",
        ];

        assert_eq!(expected_data, actual_data)
    }

    // Property test to check that `Input` roundtrips.
    #[quickcheck_macros::quickcheck]
    fn csv_roundtrip_input(inputs: Vec<Input>) -> bool {
        csv_roundtrip(inputs)
    }

    // Sanity check; this is technically already checked by the csv roundtrip
    // for any types containing `Amount`s
    #[quickcheck_macros::quickcheck]
    fn amount_string_roundtrip(amount: Amount) -> bool {
        Amount::from_str(&amount.to_string()).is_ok()
    }

    // Sometimes arithemtic on `Amount` causes its internal
    // representation to have extraneous zeroes; this is a quick
    // unit test to make sure that this does not compromise equality.
    #[test]
    fn amount_decimal_comparisons() {
        let unsafe_from_str = |x: &str| Amount::from_str(x).unwrap();
        assert_eq!(unsafe_from_str("0.00"), unsafe_from_str("0.0"));
        assert_eq!(unsafe_from_str("1.01"), unsafe_from_str("1.0100"));
        assert_eq!(unsafe_from_str("42.1030"), unsafe_from_str("42.103000"));
    }

    // Test that addition is associative on `Amount`s.
    #[quickcheck_macros::quickcheck]
    fn amount_add_associative(a: Amount, b: Amount, c: Amount) -> bool {
        let lhs = &a + &(&b + &c);
        let rhs = &(&a + &b) + &c;
        lhs == rhs
    }

    // Test that addition is commutative on `Amount`s.
    #[quickcheck_macros::quickcheck]
    fn amount_add_commutative(a: Amount, b: Amount) -> bool {
        let lhs = &a + &b;
        let rhs = &b + &a;
        lhs == rhs
    }

    // Test that the additive inverse of `Amount`s work as expected.
    #[quickcheck_macros::quickcheck]
    fn amount_additive_inverse(a: Amount) -> bool {
        &a - &a == Some(Amount::zero())
    }

    // Test that `Amount` does not allow negative string encodings.
    #[test]
    fn amount_no_negatives() {
        assert_eq!(Amount::from_str("-1"), Err(MoneyError::InvalidAmount));
    }
}
