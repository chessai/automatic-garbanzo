### Example usage
```
❯ cat test_data.csv 
type,client,tx,amount
deposit,1,1,1.0
deposit,2,2,2.0
deposit,1,3,2.0
withdrawal,1,4,1.5
withdrawal,2,5,3.0
dispute,1,1,
resolve,1,1,
chargeback,1,1,

❯ cargo run -- test_data.csv > accounts.csv
InsufficientFunds(TxID(5))
TxNotInDispute(TxID(1))

❯ cat accounts.csv
client,available,held,total,locked
1,1.5,0.0,1.5,false
2,2.0,0.00,2.0,false
```

### Design decisions
Here we'll try to cover high-level design decisions. Many smaller choices are comments in the code.

The application takes in a CSV of transactions (deposits, withdrawals) and disputes (claims, resolutions, and chargebacks), outputs to stdout the resulting state of client accounts, and outputs to stderr any errors that occured. The error reporting is not very rich, just enough for debugging, though this could easily be improved and extended to use logging library or framework.

The application runs using an in-memory [SQLite](https://www.sqlite.org/index.html) database, because the resulting code is simple, widely
understood by industry programmers, and easy to change, maintain, and expand; without the need to implement any custom relational logic. In
a real application, we might use something more like [PostgreSQL](https://www.postgresql.org/), avoiding anything in-memory. The rust library
used to interact with the SQLite database is [sqlx](https://docs.rs/sqlx/latest/sqlx/); chosen for its async compatibility (over rusqlite) and
the fact that it is not an ORM. A potential improvement not done here would be to use [sea-query](https://github.com/SeaQL/sea-query), or at least [sqlx's compile-time checked queries](https://github.com/launchbadge/sqlx#compile-time-verification), though for the purpose of this code we kept the code as static strings, because the queries are simple enough that the SQL is quite readable and easily understandable.

The async runtime chosen is tokio.

All input and output are handled with async streams, so their overhead should be constant and resistant to very large input. As such, the [csv_async](https://docs.rs/csv-async/latest/csv_async/) library is used instead of [csv](https://docs.rs/csv/latest/csv/) (which is still used in some property tests because of a [futures bug](https://github.com/rust-lang/futures-rs/issues/2090) that prevents `csv_async` from being used).

Generally throughout the application things are written in a way that is simple but easy to extend. One exception is the encoding of the data: CSV is assumed, but refactoring to allow alternate encodings would not be difficult, since it is constrained to application boundaries.

The test suite uses a combination of unit and property tests. Each test has a brief comment explaining its purpose and process for achieving that purpose.

The project is structured simply into a handful of rust files, and a SQL file (for migrations).
- [types.rs](src/types.rs): types used throughout the project
- [queries.rs](src/queries.rs): all the SQL queries used for the logic of the project
- [process.rs](src/process.rs): main logical loop of the application; reads an async stream of csv input, processes each transaction, then outputs all of the accounts to an output stream
- [cmd.rs](src/cmd.rs): parse command line options (currently just takes the input file as an argument)
- [main.rs](src/main.rs): parses command line arguments, sets up the connection to SQLite, processes the input and produces output
- [init.sql](db/000_init.sql): SQLite migrations for the `accounts` and `transactions` tables.

There are places in the code where it might be preferable in real code to write a macro for some of the more repetitive bits, but for simplicity in reading and writing, this is avoided.

There are two SQL tables in the code; `accounts` and `transactions` (n.b:`Client` has been renamed to `Account` in the code for clarity; the spec dictates a client is just some available funds, held funds, and a locked status. this seems fairly decoupled from the notion of an identity (ala KYC) and like just a single account of a client). Transactions have foreign keys into the Accounts table via their `account_id` (`client` in the spec). Disputes are not stored, for simplicity, instead; transactions track their dispute status, a simple enum, whose variants' meanings are elaborated upon in comments in the code.

The derived serde for enums with csv is slightly broken; there is more information about this in comments in the code, and there is a shim layer for some types, so that the types the project can work with are more straightforward and type-safe to work with. This is probably a place where a macro would help, as stated before.
