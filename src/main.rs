use chrono::prelude::*;
use csv::ReaderBuilder;
use std::{error::Error, fs::File};

#[derive(Debug, serde::Deserialize)]
struct Record {
    occupation_code: String,
    title: String,
    rate: f64,
}

// TODO: need to make a SQL file and add each record to it if it doesn't exist, or update it if it does

fn read_sca_rates() -> Result<(), Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
        .delimiter(b'|')
        .from_reader(File::open("2025_SCA_Rates.csv")?);

    for result in rdr.deserialize() {
        let record: Record = result?;
        println!("{:?}", record);
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let current_fiscal_year = Local::now().year() + 1;
    let sql_file = File::create_new(format!("{}_SCA_Rates.sql", current_fiscal_year))?;

    read_sca_rates()?;

    println!("All done :) またねー！");
    Ok(())
}
