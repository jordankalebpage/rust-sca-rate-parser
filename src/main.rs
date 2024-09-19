use chrono::prelude::*;
use csv::ReaderBuilder;
use std::{error::Error, fs::File, io::Write};

#[derive(Debug, serde::Deserialize)]
struct Record {
    occupation_code: String,
    title: String,
    rate: f64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let rate_records = read_sca_rates()?;
    write_sql_file(&rate_records)?;

    println!("All done :) またねー！");
    Ok(())
}

fn read_sca_rates() -> Result<Vec<Record>, Box<dyn Error>> {
    let mut rate_records = Vec::new();
    let mut rdr = ReaderBuilder::new()
        .delimiter(b'|')
        .from_reader(File::open("2025_SCA_Rates.csv")?);

    for result in rdr.deserialize() {
        let record = result?;
        rate_records.push(record);
    }

    Ok(rate_records)
}

fn write_sql_file(rate_records: &Vec<Record>) -> Result<(), Box<dyn Error>> {
    let current_fiscal_year = Local::now().year() + 1;
    let mut sql_file = File::create_new(format!("{}_SCA_Rates.sql", current_fiscal_year))?;

    writeln!(sql_file, "BEGIN TRANSACTION;\n")?;

    for record in rate_records {
        writeln!(
            sql_file,
            "IF NOT EXISTS (SELECT 1 FROM SCA_RATES WHERE OCCUPATION_CODE = '{}') THEN\n\
            \tINSERT INTO SCA_RATES (OCCUPATION_CODE, TITLE, RATE)\n\
            \tVALUES ('{}', '{}', {});\n\
            ELSE\n\
            \tUPDATE SCA_RATES\n\
            \tSET RATE = {}\n\
            \tWHERE OCCUPATION_CODE = '{}';\n\
            END IF;\n",
            record.occupation_code,
            record.occupation_code,
            record.title,
            record.rate,
            record.rate,
            record.occupation_code
        )?;
    }

    writeln!(sql_file, "COMMIT;")?;

    println!("SQL file created successfully!");
    Ok(())
}
