use clap::{App, AppSettings, Arg};

/// Constructs the main CLI interface for s3-concat.
pub fn build<'a, 'b>() -> App<'a, 'b> {
    App::new("")
        // package metadata from Cargo
        .name(env!("CARGO_PKG_NAME"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))

        // cleanup source files
        .arg(
            Arg::with_name("cleanup")
                .help("Removes source files after concatenation")
                .short("c")
                .long("cleanup")
        )

        // allow dry-run
        .arg(
            Arg::with_name("dry")
                .help("Only print out the calculated writes")
                .short("d")
                .long("dry-run")
        )

        // allow no logging
        .arg(
            Arg::with_name("quiet")
                .help("Only prints errors during execution")
                .short("q")
                .long("quiet")
        )

        // bucket argument
        .arg(
            Arg::with_name("bucket")
                .help("An S3 bucket prefix to work within")
                .index(1)
                .required(true),
        )

        // source argument
        .arg(
            Arg::with_name("source")
                .help("A source pattern to use to locate files")
                .index(2)
                .required(true),
        )

        // target argument
        .arg(
            Arg::with_name("target")
                .help("A target pattern to use to concatenate files into")
                .index(3)
                .required(true),
        )

        // settings required for parsing
        .settings(&[
            AppSettings::ArgRequiredElseHelp,
            AppSettings::HidePossibleValuesInHelp,
            AppSettings::TrailingVarArg,
        ])
}
