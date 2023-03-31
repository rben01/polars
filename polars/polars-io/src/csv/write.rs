pub use super::write_impl::CsvWriterOptions;
use super::*;

/// Writes a DataFrame as a CSV to the specified `Write`. Construct a `CsvWriter` with [`CsvWriter::new(buffer)`].
///
/// The default values of the options are below:
/// ```ignore
/// header: true
/// date_format: None
/// time_format: None
/// datetime_format: None
/// float_precision: None
/// delimiter: b','
/// quote: b'"'
/// null: String::new()
/// batch_size: 1024
/// ```
///
/// Use the `with_` methods to overwrite these options.
///
/// ## Note
/// Don't use a `Buffered` writer, the `CsvWriter` internally already buffers writes.
#[must_use]
pub struct CsvWriter<W: Write> {
    /// File or Stream handler
    buffer: W,
    /// Options to use when writing
    options: CsvWriterOptions,
}

impl<W> SerWriter<W> for CsvWriter<W>
where
    W: Write,
{
    /// Create a new `CsvWriter` with the default [`CsvWriterOptions`]
    fn new(buffer: W) -> Self {
        // 9f: all nanoseconds
        let options = CsvWriterOptions {
            time_format: Some("%T%.9f".to_string()),
            ..Default::default()
        };

        CsvWriter { buffer, options }
    }

    fn finish(&mut self, df: &mut DataFrame) -> PolarsResult<()> {
        if self.options.header {
            let names = df.get_column_names();
            write_impl::write_header(&mut self.buffer, &names, &self.options)?;
        }
        write_impl::write(&mut self.buffer, df, &self.options)
    }
}

impl<W> CsvWriter<W>
where
    W: Write,
{
    /// Set all options at once. All existing values will be overwritten by the provided `options`.
    pub fn with_options(mut self, options: CsvWriterOptions) -> Self {
        self.options = options;
        self
    }

    /// Set whether to write headers
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.options.header = has_header;
        self
    }

    /// Set the CSV file's column delimiter as a byte character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.options.delimiter = delimiter;
        self
    }

    /// Set the CSV writer's batch size, or the number of rows that will be written as a time. Larger batch sizes lead
    /// to faster writing overall but also use more memory.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.options.batch_size = batch_size;
        self
    }

    /// Set the CSV file's date format
    pub fn with_date_format(mut self, format: Option<String>) -> Self {
        if format.is_some() {
            self.options.date_format = format;
        }
        self
    }

    /// Set the CSV file's time format
    pub fn with_time_format(mut self, format: Option<String>) -> Self {
        if format.is_some() {
            self.options.time_format = format;
        }
        self
    }

    /// Set the CSV file's datetime format
    pub fn with_datetime_format(mut self, format: Option<String>) -> Self {
        if format.is_some() {
            self.options.datetime_format = format;
        }
        self
    }

    /// Set the CSV file's float precision
    pub fn with_float_precision(mut self, precision: Option<usize>) -> Self {
        if precision.is_some() {
            self.options.float_precision = precision;
        }
        self
    }

    /// Set the single byte character used for quoting
    pub fn with_quoting_char(mut self, char: u8) -> Self {
        self.options.quote = char;
        self
    }

    /// Set the CSV file's null value representation
    pub fn with_null_value(mut self, null_value: String) -> Self {
        self.options.null = null_value;
        self
    }
}
