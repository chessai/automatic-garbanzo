use crate::queries::{run_input, Pool};
use crate::types::*;

use tokio_stream::StreamExt;

#[derive(Debug)]
pub enum Error {
    Sql(crate::queries::Error),
    Io(std::io::Error),
    Csv(csv_async::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sql(sql_err) => sql_err.fmt(f),
            Self::Io(io_err) => io_err.fmt(f),
            Self::Csv(csv_err) => csv_err.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl From<crate::queries::Error> for Error {
    fn from(err: crate::queries::Error) -> Self {
        Self::Sql(err)
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Self::Sql(err.into())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<csv_async::Error> for Error {
    fn from(err: csv_async::Error) -> Self {
        Self::Csv(err)
    }
}

// - Read inputs from a CSV at input_source
// - Loop over inputs, calling `run_input`
// - Once the inputs have been exhausted, the state of Accounts is terminal
// - Write accounts as a CSV to output_sink
//
// Note: input_source and output_sink are polymorphic to allow for different
// ways of testing and the possibility of easing transition to different sources/sinks.
// The spec specifies the input_source as `P: AsRef<Path> + Send + 'static`,
// and the output_sink as `tokio::fs::stdout()`
pub async fn process<R, W>(input_source: R, output_sink: W, pool: &Pool) -> Result<(), Error>
where
    R: tokio::io::AsyncRead + Unpin + Send,
    W: tokio::io::AsyncWrite + Unpin,
{
    use crate::queries::Error as QueryError;

    // read inputs and loop over them in order, until there are none left
    {
        let mut rdr: csv_async::DeserializeRecordsIntoStream<'_, R, Input> =
            csv_async::AsyncReaderBuilder::new()
                .has_headers(true)
                .trim(csv_async::Trim::All)
                .create_deserializer(input_source)
                .into_deserialize();

        while let Some(r_input) = rdr.next().await {
            match r_input {
                Ok(input) => match run_input(&input, pool).await {
                    Ok(_) => (),
                    Err(err) => match err {
                        // SQLx Errors shouldn't occur, so we treat these as fatal.
                        QueryError::Sqlx(err) => return Err(QueryError::Sqlx(err).into()),
                        // The other failures are non-fatal and we just emit to stderr.
                        err => {
                            eprintln!("{:?}", err);
                        }
                    },
                },
                // Encountered a CSV decoding error, which we treat as fatal
                Err(err) => {
                    eprintln!("Encountered an error decoding a csv input: {:?}", err);
                    return Err(Error::Csv(err));
                }
            }
        }
    }

    // write accounts to csv
    {
        let mut ser = csv_async::AsyncWriterBuilder::new()
            .has_headers(true)
            .create_serializer(output_sink);
        let mut rows = sqlx::query_as::<_, Account>("SELECT * FROM accounts").fetch(pool);

        while let Some(row) = rows.try_next().await? {
            // csv_async::Error is considered fatal when writing, for simplicity.
            // In the spec we write to stdout, and any error when writing to stdout
            // is likely not recoverable.
            ser.serialize(row).await?
        }
    }

    Ok(())
}
