use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use regex::Regex;
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyfunction]
fn count_words_rust(file_path: &str, stopwords_path: &str, py: Python<'_>) -> PyResult<Py<PyDict>> {
    // Load stopwords into a HashSet
    let stopwords: HashSet<String> = BufReader::new(File::open(stopwords_path)?)
        .lines()
        .map(|line| line.unwrap().trim().to_lowercase())  // Normalize case & trim spaces
        .collect();

    let re = Regex::new(r"[^\w\s]").unwrap();  // Remove non-word characters
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let mut word_count: HashMap<String, usize> = HashMap::new();

    for line in reader.lines() {
        let line = line?.to_lowercase();

        // Replace non-breaking space (\xa0) with a normal space
        let fixed_line = line.replace('\u{a0}', " ");

        // Remove special characters
        let cleaned_line = re.replace_all(&fixed_line, " ");

        for word in cleaned_line.split_whitespace() {
            if !stopwords.contains(word) && !word.is_empty() {
                *word_count.entry(word.to_string()).or_insert(0) += 1;
            }
        }
    }

    // Convert the HashMap into a PyDict
    let py_dict = PyDict::new(py);
    for (key, value) in word_count {
        py_dict.set_item(key, value)?;
    }

    Ok(py_dict.into())
}

#[pymodule]
fn word_counter(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(count_words_rust, m)?)?;
    Ok(())
}