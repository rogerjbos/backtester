use tokio::spawn;
use tokio_postgres::{NoTls, Error};
use polars::prelude::*;
use std::error::Error as StdError;

#[cfg(test)]
mod tests {
    #[test]
    fn test_test() {
        // Use `super` to refer to the parent module where `summarize_performance` is defined.
        super::test();
    }
}

#[tokio::main]
async fn test() -> Result<(), Box<dyn StdError>> {
    // Connect to the database
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=postgres password=yourpassword dbname=yourdb", NoTls).await?;

    // The connection object performs the communication with the database,
    // so it is spawned off to run on its own.
    spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Execute a SELECT query
    let rows = client.query("SELECT date, ticker FROM ranks limit 5", &[]).await?;

    // Collect columns into vectors
    let mut dates = Vec::new();
    let mut tickers = Vec::new();
    for row in rows {
        let date: String = row.get(0);
        let ticker: String = row.get(1);
        ids.push(dates);
        names.push(tickers);
    }

    // Create a DataFrame from the vectors
    let df = DataFrame::new(vec![
        Series::new("date", dates),
        Series::new("ticker", tickers),
    ])?;

    // Display the DataFrame
    println!("{:?}", df);

    Ok(())
}
