use sqlx::postgres::PgPoolOptions;
use polars::prelude::*;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Assuming you have a DataFrame named `df`
    // let df = DataFrame::new(vec![])?; // Replace this with your actual DataFrame creation

   
    let df = df! (
        "nrs" => &[Some(1), Some(2), Some(3), Some(4), Some(5)],
        "names" => &[Some("foo"), Some("ham"), Some("spam"), Some("eggs"), Some("foo")],
        "groups" => &["A", "A", "B", "C", "B"],
    );

    // Database connection string
    let pg = env::var("PG").unwrap();
    let database_url = "postgresql://postgres:{pg}@192.168.86.68/tiingo";
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    // Example of inserting data from the DataFrame into PostgreSQL
    for row in df.iter_rows() {
        let your_query = sqlx::query!(
            "INSERT INTO test_tbl (nrs, names, groups) VALUES ($1, $2, $3)",
            row.get(0).unwrap().into(), // Assuming the first column is of a type that implements Into for sqlx's types
            row.get(1).unwrap().into(), // Adjust types and unwrap as necessary
            row.get(2).unwrap().into(), // Adjust types and unwrap as necessary
        );

        your_query.execute(&pool).await?;
    }

    Ok(())
}
