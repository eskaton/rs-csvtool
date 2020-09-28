#[macro_use]
extern crate clap;

use clap::{App, AppSettings, Arg, ArgGroup, ArgMatches, SubCommand};
use csv::{Reader, ReaderBuilder, Writer, WriterBuilder, ByteRecordsIter, ByteRecord};
use std::{env, process};
use std::error::Error;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::collections::HashMap;
use std::str::from_utf8;

fn get_delimiter(matches: &ArgMatches) -> u8 {
   return match matches.value_of("delimiter") {
      Some(s) => {
         if s.len() > 1 {
            error_exit("Delimiter must be a single character")
         }
         s.as_bytes()[0]
      }
      None    => b','
   };
}

struct CutConfig {
   delimiter: u8,
   columns: Option<Vec<usize>>,
   input_file: Option<String>,
   output_file: Option<String>
}

impl CutConfig {
   fn new(matches: &ArgMatches) -> CutConfig {
      let delimiter = get_delimiter(matches);
      let columns = matches.value_of("columns")
         .map(|s| parse_columns(&s.to_string()));
      let input_file = matches.value_of("input-file").map(|s| s.to_string());
      let output_file = matches.value_of("output-file").map(|s| s.to_string());

      CutConfig { delimiter, columns, input_file, output_file }
   }
}

struct ReorderConfig {
   delimiter: u8,
   columns: Option<Vec<usize>>,
   fields: Option<Vec<String>>,
   input_file: Option<String>,
   output_file: Option<String>
}

impl ReorderConfig {
   fn new(matches: &ArgMatches) -> ReorderConfig {
      let delimiter = get_delimiter(matches);
      let columns = matches.value_of("columns")
         .map(|s| parse_reorder(&s.to_string()));
      let fields = matches.value_of("fields")
         .map(|s| s.split(',').map(|s| s.to_string()).collect::<Vec<_>>());
      let input_file = matches.value_of("input-file").map(|s| s.to_string());
      let output_file = matches.value_of("output-file").map(|s| s.to_string());

      ReorderConfig { delimiter, columns, fields, input_file, output_file }
   }
}

fn error_exit(error: &str) {
   println!("{}", error);
   process::exit(1);
}

fn parse_number(str: &String) -> usize {
   return str.parse::<usize>()
      .expect(format!("Invalid number: '{}'", str).as_str());
}

fn insert_column(vector: &mut Vec<usize>, value: usize) -> () {
   let index = value as usize;

   if vector.len() < index {
      vector.resize(index, 0);
   }

   (*vector)[index - 1] = 1;
}

fn parse_columns(columns_str: &String) -> Vec<usize> {
   let mut columns : Vec<usize> = Vec::new();

   for col in columns_str.split(',').collect::<Vec<_>>().iter() {
      if col.contains('-') {
         let range : Vec<&str> = col.split('-').collect();

         if range.len() != 2 {
            error_exit(&format!("Invalid range: {}", col).to_owned());
         }

         let start = parse_number(&range.get(0).unwrap().to_string());
         let end = parse_number(&range.get(1).unwrap().to_string());

         (start..=end).for_each(|c| { insert_column(&mut columns, c); });
      } else {
         insert_column(&mut columns, parse_number(&col.to_string()));
      }
   }

   return columns;
}

fn parse_reorder(columns_str: &String) -> Vec<usize> {
   let mut columns : Vec<usize> = Vec::new();

   for column_str in columns_str.split(',').collect::<Vec<_>>().iter() {
      let column = parse_number(&column_str.to_string());

      if column < 1 {
         error_exit(&format!("Invalid column: {}", column).to_owned());
      }

      columns.push(column);
   }

   return columns;
}

fn fields_to_columns(iter: &mut ByteRecordsIter<Box<dyn BufRead>>, 
                     writer: &mut Writer<Box<dyn Write>>,
                     fields: &Vec<String>) -> Result<Vec<usize>, Box<dyn Error>> {
   let mut columns: Vec<usize> = Vec::new();
   let mut map = HashMap::new();
   let mut out_record: ByteRecord = ByteRecord::new();
   let in_record = iter.next().expect("No header line found")?;

   (0..in_record.len()).for_each(|i| {
      map.insert(from_utf8(&in_record[i]).unwrap().to_string(), i);
   });

   for field in fields {
      let column = *map.get(field)
         .expect(&format!("Column '{}' not found in header", field)
                 .to_owned()) as usize;
      columns.push(column);
      out_record.push_field(field.as_bytes());
   }

   writer.write_record(&out_record)?;

   return Ok(columns)
}

fn get_reader(input_file: &Option<String>, delimiter: u8) 
   -> Result<Reader<Box<dyn BufRead>>, Box<dyn Error>> {
   let input: Box<dyn BufRead> = match input_file {
      Some(f) => Box::new(BufReader::new(File::open(f)?)),
      None    => Box::new(BufReader::new(io::stdin()))
   };

   return Ok(ReaderBuilder::new()
      .has_headers(false)
      .delimiter(delimiter)
      .from_reader(input));
}

fn get_writer(output_file: &Option<String>, delimiter: u8) 
   -> Result<Writer<Box<dyn Write>>, Box<dyn Error>> {
   let output: Box<dyn Write> = match output_file {
      Some(f) => Box::new(BufWriter::new(File::create(f)?)),
      None    => Box::new(BufWriter::new(io::stdout()))
   };
   
   return Ok(WriterBuilder::new()
      .delimiter(delimiter)
      .from_writer(output));
}

fn cut(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
   let config = CutConfig::new(&matches);
   let columns = config.columns.unwrap();
   let mut reader: Reader<Box<dyn BufRead>> = 
      get_reader(&config.input_file, config.delimiter)?;
   let mut writer: Writer<Box<dyn Write>> = 
      get_writer(&config.output_file, config.delimiter)?;
   let mut out_record = ByteRecord::new();

   for result in reader.byte_records() {
      let record = result?;

      for idx in 0..record.len() {
         if idx < columns.len() && columns[idx] == 1 {
            out_record.push_field(&record.get(idx).unwrap());
         }
      }

      writer.write_record(&out_record)?;
      out_record.clear();
   }      

   writer.flush()?;

   Ok(())
}


fn reorder(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
   let config = ReorderConfig::new(&matches);
   let mut reader: Reader<Box<dyn BufRead>> = 
      get_reader(&config.input_file, config.delimiter)?;
   let mut writer: Writer<Box<dyn Write>> = 
      get_writer(&config.output_file, config.delimiter)?;
   let mut out_record = ByteRecord::new();
   let mut iter = reader.byte_records();
   let columns;

   if config.fields.is_some() {
      let fields = config.fields.unwrap();
      columns = fields_to_columns(&mut iter, &mut writer, &fields)?;
   } else {
      columns = config.columns.unwrap();
   }

   for result in iter {
      let record = result?;

      for idx in 0..columns.len() {
         let column_idx = columns[idx] as usize;

         if column_idx > record.len() {
            error_exit(&format!("Invalid column: {}", column_idx).to_owned());
         }

         out_record.push_field(&record.get(column_idx).unwrap());
      }

      writer.write_record(&out_record)?;
      out_record.clear();
   }      

   writer.flush()?;

   Ok(())
}

fn parse_args(args: &Vec<String>) -> ArgMatches {
   let arg_delimiter = Arg::with_name("delimiter")
           .help("Delimiter if it's not a comma")
           .short("d")
           .long("delimiter")
           .value_name("DELIMITER");

   let arg_columns = Arg::with_name("columns")
      .help("A comma separated list of column indices")
      .short("c")
      .long("columns")
      .value_name("LIST")
      .required(true)
      .conflicts_with("fields");

   let arg_fields = Arg::with_name("fields")
      .help("A comma separated list of column names")
      .short("f")
      .long("fields")
      .value_name("LIST")
      .conflicts_with("columns");

   let arg_input = Arg::with_name("input-file")
      .help("CSV file")
      .short("i")
      .long("input-file")
      .value_name("FILE");

   let arg_output = Arg::with_name("output-file")
      .help("CSV file")
      .short("o")
      .long("output-file")
      .value_name("FILE");

   return App::new("CSV tool")
      .version(crate_version!())
      .setting(AppSettings::GlobalVersion)
      .setting(AppSettings::VersionlessSubcommands)
      .subcommand(SubCommand::with_name("cut")
                  .about("Cuts out columns")
                  .arg(&arg_delimiter)
                  .arg(&arg_columns)
                  .arg(&arg_input)
                  .arg(&arg_output))
      .subcommand(SubCommand::with_name("reorder")
                  .about("Reorders columns")
                  .arg(&arg_delimiter)
                  .arg(&arg_columns)
                  .arg(&arg_fields)
                  .arg(&arg_input)
                  .arg(&arg_output)
                  .group(ArgGroup::with_name("columns or fields")
                         .args(&["columns", "fields"])
                         .required(true)))
      .setting(AppSettings::SubcommandRequiredElseHelp)
      .get_matches_from(args);
}

fn main() {
   let args: Vec<String> = env::args().collect();
   let matches = parse_args(&args);
   let result = match matches.subcommand() {
      ("cut",     Some(matches)) => cut( &matches), 
      ("reorder", Some(matches)) => reorder(&matches), 
      _                          => Ok(()), 
   };

   match result {
      Err(err) => error_exit(&err.to_string()),
      Ok(_)    => ()
   }
}
