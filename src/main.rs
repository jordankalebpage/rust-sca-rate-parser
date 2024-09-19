use chrono::prelude::*;
use csv::ReaderBuilder;
use std::{
    error::Error,
    fs::File,
    io::{BufWriter, Write},
    time::Instant,
};

#[derive(Debug, serde::Deserialize)]
struct Record {
    occupation_code: String,
    title: String,
    rate: f64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    let read_start = Instant::now();
    let rate_records = read_sca_rates()?;
    let read_duration = read_start.elapsed();

    let write_start = Instant::now();
    write_sql_file(&rate_records)?;
    let write_duration = write_start.elapsed();

    let total_duration = start.elapsed();

    println!("Read CSV time: {:?}", read_duration);
    println!("Write SQL time: {:?}", write_duration);
    println!("Total execution time: {:?}", total_duration);

    println!("All done :) またねー！");
    Ok(())
}

fn read_sca_rates() -> Result<Vec<Record>, Box<dyn Error>> {
    // CSV is 366 rows right now - doubt it gets much bigger. so just allocate 500 for now
    let mut rate_records = Vec::with_capacity(500);
    let mut rdr = ReaderBuilder::new()
        .delimiter(b'|')
        .from_reader(File::open("2025_SCA_Rates.csv")?);

    for result in rdr.deserialize() {
        let record: Record = result?;
        rate_records.push(record);
    }

    Ok(rate_records)
}

fn write_sql_file(rate_records: &[Record]) -> Result<(), Box<dyn Error>> {
    let current_fiscal_year = Local::now().year() + 1;
    let sql_file = File::create(format!("{}_SCA_Rates.sql", current_fiscal_year))?;
    let mut writer = BufWriter::new(sql_file);

    writeln!(writer, "BEGIN TRANSACTION;\n")?;

    for record in rate_records {
        writeln!(
            writer,
            "IF NOT EXISTS (SELECT 1 FROM SCA_RATES WHERE OCCUPATION_CODE = '{code}') THEN\n\
            \tINSERT INTO SCA_RATES (OCCUPATION_CODE, TITLE, RATE)\n\
            \tVALUES ('{code}', '{title}', {rate});\n\
            ELSE\n\
            \tUPDATE SCA_RATES\n\
            \tSET RATE = {rate}\n\
            \tWHERE OCCUPATION_CODE = '{code}';\n\
            END IF;\n",
            code = record.occupation_code,
            title = record.title,
            rate = record.rate
        )?;
    }

    writeln!(writer, "COMMIT;")?;
    writer.flush()?;

    println!("SQL file created successfully!");
    Ok(())
}
