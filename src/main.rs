use csv::ReaderBuilder;
use std::{error::Error, fs::File, process};

#[derive(Debug, serde::Deserialize)]
struct Record {
    occupation_code: String,
    title: String,
    rate: f64,
}

fn read_sca_rates() -> Result<(), Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
        .delimiter(b'|')
        .from_reader(File::open("2025_SCA_Rates.csv")?);

    for result in rdr.deserialize() {
        let record: Record = result?;
        println!("{:?}", record);
    }

    println!("All done :) またねー！");
    Ok(())
}

fn main() {
    if let Err(err) = read_sca_rates() {
        println!("error: {}", err);
        process::exit(1);
    }
}
