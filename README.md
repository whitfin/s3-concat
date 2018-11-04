# S3 Concat
[![Crates.io](https://img.shields.io/crates/v/s3-concat.svg)](https://crates.io/crates/s3-concat) [![Build Status](https://img.shields.io/travis/whitfin/s3-concat.svg)](https://travis-ci.org/whitfin/s3-concat)

A small utility to concatenate files in AWS S3. Design to be simple and quick, this tool uses the Multipart Upload API provided by AWS to concatenate files. This avoids the need to download files to the local machines, although it does come with caveats. S3 interaction is controlled by [rusoto_s3](https://crates.io/crates/rusoto_s3), so check out those docs for authorization practices.

## Installation

You can install `s3-concat` from either this repository, or from Crates (once it's published):

```shell
# install from Cargo
$ cargo install s3-concat

# install the latest from GitHub
$ cargo install --git https://github.com/whitfin/s3-concat.git
```

## Usage

Credentials can be configured by following the instructions on the [AWS Documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-environment.html), although examples will use environment variables for the sake of clarity.

You can concatenate files in a basic manner just by providing a source pattern, and a target file path:

```shell
$ AWS_ACCESS_KEY_ID=MY_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=MY_SECRET_ACCESS_KEY \
    AWS_DEFAULT_REGION=us-west-2 \
    s3-concat my.bucket.name 'archives/*.gz' 'archive.gz'
```

If the case you're working with long paths, you can add a prefix on the bucket name to avoid having to type it all out multiple times. In the following case, `*.gz` and `archive.gz` are relative to the `my/annoyingly/nested/path/` prefix.

```shell
$ AWS_ACCESS_KEY_ID=MY_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=MY_SECRET_ACCESS_KEY \
    AWS_DEFAULT_REGION=us-west-2 \
    s3-concat my.bucket.name/my/annoyingly/nested/path/ '*.gz' 'archive.gz'
```

You can also use pattern matching (driven by the official `regex` crate), to use segments of the source paths in your target paths. Here is an example of mapping a date hierarchy (`YYYY/MM/DD`) to a flat structure (`YYYY-MM-DD`):

```shell
$ AWS_ACCESS_KEY_ID=MY_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=MY_SECRET_ACCESS_KEY \
    AWS_DEFAULT_REGION=us-west-2 \
    s3-concat my.bucket.name 'date-hierachy/(\d{4})/(\d{2})/(\d{2})/*.gz' 'flat-hierarchy/$1-$2-$3.gz'
```

In this case, all files in `2018/01/01/*` would be mapped to `2018-01-01.gz`. Don't forget to add single quotes around your expressions to avoid any pesky shell expansions!

For any other functionality, check out the help menu (although this example below might be outdated):

```shell
$ s3-concat -h
s3-concat 1.0.0
Isaac Whitfield <iw@whitfin.io>
Concatenate Amazon S3 files remotely using flexible patterns

USAGE:
    s3-concat [FLAGS] <bucket> <source> <target>

FLAGS:
    -c, --cleanup    Removes source files after concatenation
    -d, --dry-run    Only print out the calculated writes
    -h, --help       Prints help information
    -q, --quiet      Only prints errors during execution
    -V, --version    Prints version information

ARGS:
    <bucket>    An S3 bucket prefix to work within
    <source>    A source pattern to use to locate files
    <target>    A target pattern to use to concatenate files into
```

## Limitations

In order to concatenate files remotely (i.e. without pulling them to your machine), this tool uses the Multipart Upload API of S3. This means that all limitations of that API are inherited by this tool. Usually this is a non-issue, but one of the more noticable problems is that files smaller than 5MB cannot be concatenated. To avoid wasted AWS calls, this is currently caught in the client layer and will result in a client side error. Due to the complexity in working around this, it's currently unsupported to join files with a size smaller than 5MB.
