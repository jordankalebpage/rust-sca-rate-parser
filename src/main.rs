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
    description: String,
    title: String,
    rate: f64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    let read_new_start = Instant::now();
    let mut rate_records = read_sca_rates()?;
    let read_new_duration = read_new_start.elapsed();

    let read_prev_start = Instant::now();
    rate_records = read_previous_rates_for_descriptions(rate_records)?;
    let read_prev_duration = read_prev_start.elapsed();

    let write_start = Instant::now();
    write_sql_file(&rate_records)?;
    let write_duration = write_start.elapsed();

    let total_duration = start.elapsed();

    println!("Read New SCA Rates CSV time: {:?}", read_new_duration);
    println!("Read Previous SCA Rates CSV time: {:?}", read_prev_duration);
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

fn read_previous_rates_for_descriptions(
    mut rate_records: Vec<Record>,
) -> Result<Vec<Record>, Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new().from_reader(File::open("2023_sca_rates_export_arrs.csv")?);

    for result in rdr.deserialize() {
        let record: Record = result?;
        for rate_record in &mut rate_records {
            if rate_record.occupation_code == record.occupation_code {
                rate_record.description = record.description;
                break;
            }
        }
    }

    Ok(rate_records)
}

fn write_sql_file(rate_records: &[Record]) -> Result<(), Box<dyn Error>> {
    let current_fiscal_year = Local::now().year() + 1;
    let sql_file = File::create(format!(
        "V1.1.114__Insert_{}_SCA_Rates.sql",
        current_fiscal_year
    ))?;
    let mut writer = BufWriter::new(sql_file);

    writeln!(
        writer,
        "IF NOT EXISTS (SELECT 1 FROM Jobs WHERE YEAR = {year})\n\
        BEGIN\n",
        year = current_fiscal_year
    )?;

    for record in rate_records {
        writeln!(writer,
        "INSERT INTO dbo.Jobs (JobGuid, JobCode, JobTitle, Description, HourlyWageRate, IsSCAJob, CreatedDate, Year)\n\
        VALUES (NEWID(), '{code}', '{title}', '{description}', {rate}, 1, GETUTCDATE(), {year})\n",
        code = record.occupation_code,
        title = record.title,
        description = record.description,
        rate = record.rate,
        year = current_fiscal_year)?;
    }

    writeln!(writer, "END;")?;
    writer.flush()?;

    println!("SQL file created successfully!");
    Ok(())
}
