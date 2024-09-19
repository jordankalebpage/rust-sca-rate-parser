use std::fs::File;

#[derive(Debug, serde::Deserialize)]
struct Record {
    ocupation_code: String,
    title: String,
    footnote: String,
    rate: f64,
}

fn main() {}
