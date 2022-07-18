// This might be a bit overkill for just reading argv[1], but
// clap is the most extensible option, and it is less code than
// implementing the logic ourselves if we derive everything.
#[derive(clap::Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Config {
    #[clap(value_parser)]
    pub input_file: std::path::PathBuf,
}
