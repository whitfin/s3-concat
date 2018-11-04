//! Concatenate Amazon S3 files remotely using flexible patterns.
//!
//! This tool should be used from a command line and can be used to join
//! files remotely in an S3 filesystem through concatenation.
//!
//! Credentials must be provided via guidelines in the [AWS Documentation]
//! (https://docs.aws.amazon.com/cli/latest/userguide/cli-environment.html).
extern crate clap;
extern crate env_logger;
extern crate quick_xml;
extern crate regex;
extern crate rusoto_core;
extern crate rusoto_s3;

use regex::Regex;
use rusoto_core::{credential::ChainProvider, region::Region, HttpClient};
use rusoto_s3::*;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

mod cli;
mod types;

fn main() -> types::ConcatResult<()> {
    // initialize for rusoto
    env_logger::init();

    // build the CLI and grab all argumentss
    let args = cli::build().get_matches();

    // parse the bucket argument
    let mut splitn = args
        .value_of("bucket")
        .unwrap()
        .trim_left_matches("s3://")
        .splitn(2, '/');

    // bucket is required, prefix is optional after `/`
    let bucket = Cow::from(splitn.next().unwrap().to_string());
    let prefix = Cow::from(
        splitn
            .next()
            .unwrap_or("")
            .trim_right_matches('/')
            .to_string(),
    );

    // unpack the dry run argument
    let dryrun = args.is_present("dry");

    // unwrap and compile the source regex (unwrap should be safe)
    let source = Regex::new(&args.value_of("source").unwrap())?;
    let target = Cow::from(args.value_of("target").unwrap());

    // create client options
    let client = HttpClient::new()?;
    let region = Region::default();

    // create provided with timeout
    let mut chain = ChainProvider::new();
    chain.set_timeout(Duration::from_millis(500));

    // construct new S3 client
    let s3 = S3Client::new_with(client, chain, region);

    // sources and target -> upload mappings
    let mut sources: HashMap<String, HashSet<String>> = HashMap::new();
    let mut targets: HashMap<String, String> = HashMap::new();

    // construct uploads - this is separate to allow easy
    // handling of errors being returned (and cleanup)
    let result = construct_uploads(
        dryrun,
        &s3,
        bucket.clone(),
        prefix,
        target,
        source,
        &mut sources,
        &mut targets,
    );

    // dry doesn't post-process
    if dryrun {
        return Ok(());
    }

    // handle errors
    if result.is_err() {
        // try to abort all requests
        for (key, upload_id) in targets {
            abort_request(
                &s3,
                key.to_string(),
                bucket.to_string(),
                upload_id.to_string(),
            );
        }

        // passthrough
        return result;
    }

    // attempt to complete all requests
    for (key, upload_id) in targets {
        // log out to be user friendly...
        println!("Completing {}...", upload_id);

        // create a request to list parts buffer
        let parts = ListPartsRequest {
            key: key.to_string(),
            bucket: bucket.to_string(),
            upload_id: upload_id.to_string(),
            ..ListPartsRequest::default()
        };

        // carry out the request for the parts list
        let parts_result = s3.list_parts(parts).sync();

        // attempt to list the pending parts
        if let Err(err) = parts_result {
            // if we can't list the parts, tell the user to help out
            eprintln!("Unable to list pending parts for {}: {}", upload_id, err);

            // gotta abort
            abort_request(
                &s3,
                key.to_string(),
                bucket.to_string(),
                upload_id.to_string(),
            );

            // move on
            continue;
        }

        // buffer up all completed parts
        let completed = parts_result
            .unwrap()
            .parts
            .unwrap()
            .into_iter()
            .map(|part| CompletedPart {
                e_tag: part.e_tag,
                part_number: part.part_number,
            }).collect();

        // create our multipart completion body
        let multipart = CompletedMultipartUpload {
            parts: Some(completed),
        };

        // create our multipart completion request
        let complete = CompleteMultipartUploadRequest {
            key: key.to_string(),
            bucket: bucket.to_string(),
            upload_id: upload_id.to_string(),
            multipart_upload: Some(multipart),
            ..CompleteMultipartUploadRequest::default()
        };

        // attempt to complete each request, abort on fail (can't short circut)
        if let Err(_) = s3.complete_multipart_upload(complete).sync() {
            // remove the upload sources
            sources.remove(&key);

            // abort now!
            abort_request(
                &s3,
                key.to_string(),
                bucket.to_string(),
                upload_id.to_string(),
            );
        }
    }

    // iterate all upload sources
    for keys in sources.values() {
        // iterate all concat'ed
        for key in keys {
            // print that we're removing
            println!("Removing {}...", key);

            // create the removal request
            let delete = DeleteObjectRequest {
                key: key.to_string(),
                bucket: bucket.to_string(),
                ..DeleteObjectRequest::default()
            };

            // attemp to remove the objects from S3
            if let Err(_) = s3.delete_object(delete).sync() {
                eprintln!("Unable to remove {}", key);
            }
        }
    }

    // passthrough
    result
}

/// Constructs all upload requests based on walking the S3 tree.
///
/// This will populate the provided mappings, as they're using in the main
/// function for error handling (this allows us to use ? in this function).
fn construct_uploads<'a>(
    dry: bool,
    s3: &S3Client,
    bucket: Cow<'a, str>,
    prefix: Cow<'a, str>,
    target: Cow<'a, str>,
    pattern: Regex,
    sources: &mut HashMap<String, HashSet<String>>,
    targets: &mut HashMap<String, String>,
) -> types::ConcatResult<()> {
    // iteration token
    let mut token = None;

    loop {
        // create a request to list objects
        let request = ListObjectsV2Request {
            bucket: bucket.to_string(),
            prefix: if prefix.is_empty() {
                None
            } else {
                Some(prefix.to_string())
            },
            continuation_token: token.clone(),
            ..ListObjectsV2Request::default()
        };

        // execute the request and await the response (blocking)
        let response = s3.list_objects_v2(request).sync()?;

        // check contents (although should always be there)
        if response.contents.is_none() {
            continue;
        }

        // iterate all objects
        for entry in response.contents.unwrap() {
            // unwrap the source key
            let key = entry.key.unwrap();

            // skip non-matching files
            if !pattern.is_match(&key) {
                continue;
            }

            // AWS doesn't let us concat < 5MB
            if entry.size.unwrap() < 5000000 {
                return Err(format!("Unable to concat files below 5MB: {}", key).into());
            }

            // format the source path, as well as the target
            let part_source = format!("{}/{}", bucket, key);
            let full_target = pattern
                .replace_all(&key, target.to_string().as_str())
                .to_string();

            // don't concat into self
            if full_target == key {
                continue;
            }

            // log out exactly what we're concatenating right now
            println!("Concatenating {} -> {}", key, full_target);

            // skip
            if dry {
                continue;
            }

            // ensure we have an upload identifier
            if !targets.contains_key(&full_target) {
                // initialize the upload request as needed
                let creation = CreateMultipartUploadRequest {
                    bucket: bucket.to_string(),
                    key: full_target.to_string(),
                    ..CreateMultipartUploadRequest::default()
                };

                // init the request against AWS, and retrieve the identifier
                let created = s3.create_multipart_upload(creation).sync()?;
                let upload = created.upload_id.expect("upload id should exist");

                // insert the upload identifier against the target
                targets.insert(full_target.clone(), upload.clone());
                sources.insert(upload, HashSet::new());
            };

            // retrieve the upload identifier for the target
            let upload_id = targets
                .get(&full_target)
                .expect("upload identifier should always be mapped");

            // retrieve the sources list for the upload_id
            let sources = sources.get_mut(&*upload_id).unwrap();

            // create the copy request for the existing key
            let copy_request = UploadPartCopyRequest {
                bucket: bucket.to_string(),
                copy_source: part_source,
                part_number: (sources.len() + 1) as i64,
                key: full_target,
                upload_id: upload_id.to_string(),
                ..UploadPartCopyRequest::default()
            };

            // carry out the request for the part copy
            s3.upload_part_copy(copy_request).sync()?;

            // push the source for removal
            sources.insert(key);
        }

        // break if there's no way to continue
        if let None = response.next_continuation_token {
            break;
        }

        // store the token for next iteration
        token = response.next_continuation_token;
    }

    Ok(())
}

/// Aborts a multipart request in S3 by upload_id.
///
/// This can be used to abort a failed upload request, due to either the inability
/// to construct the part request, or the inability to complete the multi request.
fn abort_request(s3: &S3Client, key: String, bucket: String, upload_id: String) {
    // print that it's being aborted
    eprintln!("Aborting {}...", upload_id);

    // create the main abort request
    let abort = AbortMultipartUploadRequest {
        key: key.to_string(),
        bucket: bucket.to_string(),
        upload_id: upload_id.to_string(),
        ..AbortMultipartUploadRequest::default()
    };

    // attempt to abort each request, log on fail (can't short circut)
    if let Err(_) = s3.abort_multipart_upload(abort).sync() {
        eprintln!("Unable to abort: {}", upload_id);
    }
}
