// The test in this file runs the server in a separate thread and makes HTTP requests as a smoke
// test for the integration of the whole system.
//
// As written, only one test of this style can run at a time. Add more data to the existing test to
// test more scenarios rather than adding more tests in the same style.
//
// Or, change the way this test behaves to create isolated instances by:
//
// - Finding an unused port for the server to run on and using that port in the URL
// - Creating a temporary directory for an isolated database path
//
// Or, change the tests to use one server and isolate through `org_id` by:
//
// - Starting one server before all the relevant tests are run
// - Creating a unique org_id per test
// - Stopping the server after all relevant tests are run

use assert_cmd::prelude::*;
use futures::prelude::*;
use generated_types::{
    aggregate::AggregateType,
    node::{Comparison, Type as NodeType, Value},
    read_group_request::Group,
    read_response::{frame::Data, *},
    storage_client::StorageClient,
    Aggregate, MeasurementFieldsRequest, MeasurementNamesRequest, MeasurementTagKeysRequest,
    MeasurementTagValuesRequest, Node, Predicate, ReadFilterRequest, ReadGroupRequest, ReadSource,
    Tag, TagKeysRequest, TagValuesRequest, TimestampRange,
};
use prost::Message;
use std::convert::TryInto;
use std::fs;
use std::process::{Child, Command};
use std::str;
use std::time::{Duration, SystemTime};
use std::u32;
use tempfile::TempDir;
use test_helpers::*;

const HTTP_BASE: &str = "http://localhost:8080";
const API_BASE: &str = "http://localhost:8080/api/v2";
const GRPC_URL_BASE: &str = "http://localhost:8082/";
const TOKEN: &str = "InfluxDB IOx doesn't have authentication yet";

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T, E = Error> = std::result::Result<T, E>;

async fn read_data_as_sql(
    client: &reqwest::Client,
    path: &str,
    org_id: &str,
    bucket_id: &str,
    sql_query: &str,
) -> Result<Vec<String>> {
    let url = format!("{}{}", API_BASE, path);
    let lines = client
        .get(&url)
        .query(&[
            ("bucket", bucket_id),
            ("org", org_id),
            ("sql_query", sql_query),
        ])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?
        .trim()
        .split('\n')
        .map(str::to_string)
        .collect();
    Ok(lines)
}

async fn write_data(
    client: &influxdb2_client::Client,
    org_id: &str,
    bucket_id: &str,
    points: Vec<influxdb2_client::DataPoint>,
) -> Result<()> {
    client
        .write(org_id, bucket_id, stream::iter(points))
        .await?;
    Ok(())
}

#[tokio::test]
async fn read_and_write_data() -> Result<()> {
    let mut server = TestServer::new()?;
    server.wait_until_ready().await;

    let org_id_str = "0000111100001111";
    let org_id = u64::from_str_radix(org_id_str, 16).unwrap();
    let bucket_id_str = "1111000011110000";
    let bucket_id = u64::from_str_radix(bucket_id_str, 16).unwrap();

    let client = reqwest::Client::new();
    let client2 = influxdb2_client::Client::new(HTTP_BASE, TOKEN);

    let start_time = SystemTime::now();
    let ns_since_epoch: i64 = start_time
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time should have been after the epoch")
        .as_nanos()
        .try_into()
        .expect("Unable to represent system time");

    // TODO: make a more extensible way to manage data for tests, such as in external fixture
    // files or with factories.
    let points = vec![
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .tag("region", "us-west")
            .field("value", 0.64)
            .timestamp(ns_since_epoch)
            .build()?,
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .field("value", 27.99)
            .timestamp(ns_since_epoch + 1)
            .build()?,
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server02")
            .tag("region", "us-west")
            .field("value", 3.89)
            .timestamp(ns_since_epoch + 2)
            .build()?,
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .tag("region", "us-east")
            .field("value", 1234567.891011)
            .timestamp(ns_since_epoch + 3)
            .build()?,
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .tag("region", "us-west")
            .field("value", 0.000003)
            .timestamp(ns_since_epoch + 4)
            .build()?,
        influxdb2_client::DataPoint::builder("system")
            .tag("host", "server03")
            .field("uptime", 1303385)
            .timestamp(ns_since_epoch + 5)
            .build()?,
        influxdb2_client::DataPoint::builder("swap")
            .tag("host", "server01")
            .tag("name", "disk0")
            .field("in", 3)
            .field("out", 4)
            .timestamp(ns_since_epoch + 6)
            .build()?,
        influxdb2_client::DataPoint::builder("status")
            .field("active", true)
            .timestamp(ns_since_epoch + 7)
            .build()?,
        influxdb2_client::DataPoint::builder("attributes")
            .field("color", "blue")
            .timestamp(ns_since_epoch + 8)
            .build()?,
    ];
    write_data(&client2, org_id_str, bucket_id_str, points).await?;

    let expected_read_data = substitute_nanos(
        ns_since_epoch,
        &[
            "+----------+---------+---------------------+----------------+",
            "| host     | region  | time                | value          |",
            "+----------+---------+---------------------+----------------+",
            "| server01 | us-west | ns0 | 0.64           |",
            "| server01 |         | ns1 | 27.99          |",
            "| server02 | us-west | ns2 | 3.89           |",
            "| server01 | us-east | ns3 | 1234567.891011 |",
            "| server01 | us-west | ns4 | 0.000003       |",
            "+----------+---------+---------------------+----------------+",
        ],
    );

    let text = read_data_as_sql(
        &client,
        "/read",
        org_id_str,
        bucket_id_str,
        "select * from cpu_load_short",
    )
    .await?;
    assert_eq!(
        text, expected_read_data,
        "Actual:\n{:#?}\nExpected:\n{:#?}",
        text, expected_read_data
    );

    // Make an invalid organization WAL dir to test that the server ignores it instead of crashing
    let invalid_org_dir = server.dir.path().join("not-an-org-id");
    fs::create_dir(invalid_org_dir)?;

    // Test the WAL by restarting the server
    server.restart()?;
    server.wait_until_ready().await;

    // Then check the entries are restored from the WAL

    let text = read_data_as_sql(
        &client,
        "/read",
        org_id_str,
        bucket_id_str,
        "select * from cpu_load_short",
    )
    .await?;
    assert_eq!(text, expected_read_data);

    let mut storage_client = StorageClient::connect(GRPC_URL_BASE).await?;

    // Validate that capabilities rpc endpoint is hooked up
    let capabilities_response = storage_client.capabilities(()).await?;
    let capabilities_response = capabilities_response.into_inner();
    assert_eq!(
        capabilities_response.caps.len(),
        1,
        "Response: {:?}",
        capabilities_response
    );

    let partition_id = u64::from(u32::MAX);
    let read_source = ReadSource {
        org_id,
        bucket_id,
        partition_id,
    };
    let mut d = Vec::new();
    read_source.encode(&mut d)?;
    let read_source = prost_types::Any {
        type_url: "/TODO".to_string(),
        value: d,
    };
    let read_source = Some(read_source);

    let range = TimestampRange {
        start: ns_since_epoch,
        end: ns_since_epoch + 10,
    };
    let range = Some(range);

    let predicate = Predicate {
        root: Some(Node {
            node_type: NodeType::ComparisonExpression as i32,
            children: vec![
                Node {
                    node_type: NodeType::TagRef as i32,
                    children: vec![],
                    value: Some(Value::TagRefValue("host".into())),
                },
                Node {
                    node_type: NodeType::Literal as i32,
                    children: vec![],
                    value: Some(Value::StringValue("server01".into())),
                },
            ],
            value: Some(Value::Comparison(Comparison::Equal as _)),
        }),
    };
    let predicate = Some(predicate);

    let read_filter_request = tonic::Request::new(ReadFilterRequest {
        read_source: read_source.clone(),
        range: range.clone(),
        predicate: predicate.clone(),
    });
    let read_response = storage_client.read_filter(read_filter_request).await?;

    let responses: Vec<_> = read_response.into_inner().try_collect().await?;
    let frames: Vec<Data> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    assert_eq!(frames.len(), 10);

    let expected_frames = substitute_nanos(ns_since_epoch, &[
        "SeriesFrame, tags: _field=value,_measurement=cpu_load_short,host=server01,region=, type: 0",
        "FloatPointsFrame, timestamps: [ns1], values: \"27.99\"",
        "SeriesFrame, tags: _field=value,_measurement=cpu_load_short,host=server01,region=us-east, type: 0",
        "FloatPointsFrame, timestamps: [ns3], values: \"1234567.891011\"",
        "SeriesFrame, tags: _field=value,_measurement=cpu_load_short,host=server01,region=us-west, type: 0",
        "FloatPointsFrame, timestamps: [ns0, ns4], values: \"0.64,0.000003\"",
        "SeriesFrame, tags: _field=in,_measurement=swap,host=server01,name=disk0, type: 1",
        "IntegerPointsFrame, timestamps: [ns6], values: \"3\"",
        "SeriesFrame, tags: _field=out,_measurement=swap,host=server01,name=disk0, type: 1",
        "IntegerPointsFrame, timestamps: [ns6], values: \"4\""
    ]);

    let actual_frames = dump_data_frames(&frames);

    assert_eq!(
        expected_frames,
        actual_frames,
        "Expected:\n{}\nActual:\n{}",
        expected_frames.join("\n"),
        actual_frames.join("\n")
    );

    let tag_keys_request = tonic::Request::new(TagKeysRequest {
        tags_source: read_source.clone(),
        range: range.clone(),
        predicate: predicate.clone(),
    });

    let tag_keys_response = storage_client.tag_keys(tag_keys_request).await?;
    let responses: Vec<_> = tag_keys_response.into_inner().try_collect().await?;

    let keys = &responses[0].values;
    let keys: Vec<_> = keys.iter().map(|s| str::from_utf8(s).unwrap()).collect();

    assert_eq!(
        keys,
        vec!["_field", "_measurement", "host", "name", "region"]
    );

    let tag_values_request = tonic::Request::new(TagValuesRequest {
        tags_source: read_source.clone(),
        range: range.clone(),
        predicate: predicate.clone(),
        tag_key: String::from("host"),
    });

    let tag_values_response = storage_client.tag_values(tag_values_request).await?;
    let responses: Vec<_> = tag_values_response.into_inner().try_collect().await?;

    let values = &responses[0].values;
    let values: Vec<_> = values.iter().map(|s| str::from_utf8(s).unwrap()).collect();

    assert_eq!(values, vec!["server01"]);

    // Note: it is almost certain that the read group response is incorrect
    let read_group_request = tonic::Request::new(ReadGroupRequest {
        read_source: read_source.clone(),
        range: range.clone(),
        predicate: predicate.clone(),
        group_keys: vec![String::from("region")],
        group: Group::By as _,
        aggregate: Some(Aggregate {
            r#type: AggregateType::Sum as i32,
        }),
        hints: 0,
    });
    let read_group_response = storage_client.read_group(read_group_request).await?;

    let responses: Vec<_> = read_group_response.into_inner().try_collect().await?;

    let frames: Vec<_> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    let expected_group_frames = substitute_nanos(ns_since_epoch, &[
        "GroupFrame, tag_keys: region, partition_key_vals: ",
        "SeriesFrame, tags: _field=value,_measurement=cpu_load_short,region=,host=server01, type: 0",
        "FloatPointsFrame, timestamps: [ns1], values: \"27.99\"",
        "GroupFrame, tag_keys: region, partition_key_vals: us-east",
        "SeriesFrame, tags: _field=value,_measurement=cpu_load_short,region=us-east,host=server01, type: 0",
        "FloatPointsFrame, timestamps: [ns3], values: \"1234567.891011\"",
        "GroupFrame, tag_keys: region, partition_key_vals: us-west",
        "SeriesFrame, tags: _field=value,_measurement=cpu_load_short,region=us-west,host=server01, type: 0",
        "FloatPointsFrame, timestamps: [ns0, ns4], values: \"0.64,0.000003\""
    ]);

    let actual_group_frames = dump_data_frames(&frames);

    assert_eq!(
        expected_group_frames,
        actual_group_frames,
        "Expected:\n{}\nActual:\n{}",
        expected_group_frames.join("\n"),
        actual_group_frames.join("\n")
    );

    let measurement_names_request = tonic::Request::new(MeasurementNamesRequest {
        source: read_source.clone(),
        range: range.clone(),
        predicate: None,
    });

    let measurement_names_response = storage_client
        .measurement_names(measurement_names_request)
        .await?;
    let responses: Vec<_> = measurement_names_response
        .into_inner()
        .try_collect()
        .await?;

    let values = &responses[0].values;
    let values: Vec<_> = values.iter().map(|s| str::from_utf8(s).unwrap()).collect();

    assert_eq!(
        values,
        vec!["attributes", "cpu_load_short", "status", "swap", "system"]
    );

    let measurement_tag_keys_request = tonic::Request::new(MeasurementTagKeysRequest {
        source: read_source.clone(),
        measurement: String::from("cpu_load_short"),
        range: range.clone(),
        predicate: predicate.clone(),
    });

    let measurement_tag_keys_response = storage_client
        .measurement_tag_keys(measurement_tag_keys_request)
        .await?;
    let responses: Vec<_> = measurement_tag_keys_response
        .into_inner()
        .try_collect()
        .await?;

    let values = &responses[0].values;
    let values: Vec<_> = values.iter().map(|s| str::from_utf8(s).unwrap()).collect();

    assert_eq!(values, vec!["_field", "_measurement", "host", "region"]);

    let measurement_tag_values_request = tonic::Request::new(MeasurementTagValuesRequest {
        source: read_source.clone(),
        measurement: String::from("cpu_load_short"),
        tag_key: String::from("host"),
        range: range.clone(),
        predicate: predicate.clone(),
    });

    let measurement_tag_values_response = storage_client
        .measurement_tag_values(measurement_tag_values_request)
        .await?;
    let responses: Vec<_> = measurement_tag_values_response
        .into_inner()
        .try_collect()
        .await?;

    let values = &responses[0].values;
    let values: Vec<_> = values.iter().map(|s| str::from_utf8(s).unwrap()).collect();

    assert_eq!(values, vec!["server01"]);

    let measurement_fields_request = tonic::Request::new(MeasurementFieldsRequest {
        source: read_source.clone(),
        measurement: String::from("cpu_load_short"),
        range: range.clone(),
        predicate: predicate.clone(),
    });

    let measurement_fields_response = storage_client
        .measurement_fields(measurement_fields_request)
        .await?;
    let responses: Vec<_> = measurement_fields_response
        .into_inner()
        .try_collect()
        .await?;

    let fields = &responses[0].fields;
    assert_eq!(fields.len(), 1);

    let field = &fields[0];
    assert_eq!(field.key, "value");
    assert_eq!(field.r#type, DataType::Float as i32);
    assert_eq!(field.timestamp, ns_since_epoch + 4);

    test_http_error_messages(&client2).await?;

    Ok(())
}

// Don't make a separate #test function so that we can reuse the same
// server process
async fn test_http_error_messages(client: &influxdb2_client::Client) -> Result<()> {
    // send malformed request (bucket id is invalid)
    let result = client
        .write_line_protocol("Bar", "Foo", "arbitrary")
        .await
        .expect_err("Should have errored");

    let expected_error = "HTTP request returned an error: 400 Bad Request, `{\"error\":\"Error parsing line protocol: A generic parsing error occurred: TakeWhile1\"}`";
    assert_eq!(result.to_string(), expected_error);

    Ok(())
}

/// substitutes "ns" --> ns_since_epoch, ns1-->ns_since_epoch+1, etc
fn substitute_nanos(ns_since_epoch: i64, lines: &[&str]) -> Vec<String> {
    let substitutions = vec![
        ("ns0", format!("{}", ns_since_epoch)),
        ("ns1", format!("{}", ns_since_epoch + 1)),
        ("ns2", format!("{}", ns_since_epoch + 2)),
        ("ns3", format!("{}", ns_since_epoch + 3)),
        ("ns4", format!("{}", ns_since_epoch + 4)),
        ("ns5", format!("{}", ns_since_epoch + 5)),
        ("ns6", format!("{}", ns_since_epoch + 6)),
    ];

    lines
        .iter()
        .map(|line| {
            let mut line = line.to_string();
            for (from, to) in &substitutions {
                line = line.replace(from, to);
            }
            line
        })
        .collect()
}

struct TestServer {
    server_process: Child,

    // The temporary directory **must** be last so that it is
    // dropped after the database closes.
    #[allow(dead_code)]
    dir: TempDir,
}

impl TestServer {
    fn new() -> Result<Self> {
        let _ = dotenv::dotenv(); // load .env file if present

        let dir = test_helpers::tmp_dir()?;

        let server_process = Command::cargo_bin("influxdb_iox")?
            // Can enable for debbugging
            //.arg("-vv")
            .env("INFLUXDB_IOX_DB_DIR", dir.path())
            .spawn()?;

        Ok(Self {
            dir,
            server_process,
        })
    }

    fn restart(&mut self) -> Result<()> {
        self.server_process.kill()?;
        self.server_process.wait()?;
        self.server_process = Command::cargo_bin("influxdb_iox")?
            // Can enable for debbugging
            //.arg("-vv")
            .env("INFLUXDB_IOX_DB_DIR", self.dir.path())
            .spawn()?;
        Ok(())
    }

    async fn wait_until_ready(&self) {
        // Poll the RPC and HTTP servers separately as they listen on
        // different ports but both need to be up for the test to run
        let try_grpc_connect = async {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                match StorageClient::connect(GRPC_URL_BASE).await {
                    Ok(storage_client) => {
                        println!(
                            "Successfully connected storage_client: {:?}",
                            storage_client
                        );
                        return;
                    }
                    Err(e) => {
                        println!("Waiting for gRPC server to be up: {}", e);
                    }
                }
                interval.tick().await;
            }
        };

        let try_http_connect = async {
            let client = reqwest::Client::new();
            let url = format!("{}/ping", HTTP_BASE);
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                match client.get(&url).send().await {
                    Ok(resp) => {
                        println!("Successfully got a response from HTTP: {:?}", resp);
                        return;
                    }
                    Err(e) => {
                        println!("Waiting for HTTP server to be up: {}", e);
                    }
                }
                interval.tick().await;
            }
        };

        let pair = future::join(try_http_connect, try_grpc_connect);

        let capped_check = tokio::time::timeout(Duration::from_secs(3), pair);

        match capped_check.await {
            Ok(_) => println!("Server is up correctly"),
            Err(e) => println!("WARNING: server was not ready: {}", e),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server_process
            .kill()
            .expect("Should have been able to kill the test server");
    }
}

fn dump_data_frames(frames: &[Data]) -> Vec<String> {
    frames.iter().map(|f| dump_data(f)).collect()
}

fn dump_data(data: &Data) -> String {
    match Some(data) {
        Some(Data::Series(SeriesFrame { tags, data_type })) => format!(
            "SeriesFrame, tags: {}, type: {:?}",
            dump_tags(tags),
            data_type
        ),
        Some(Data::FloatPoints(FloatPointsFrame { timestamps, values })) => format!(
            "FloatPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::IntegerPoints(IntegerPointsFrame { timestamps, values })) => format!(
            "IntegerPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::BooleanPoints(BooleanPointsFrame { timestamps, values })) => format!(
            "BooleanPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::StringPoints(StringPointsFrame { timestamps, values })) => format!(
            "StringPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::Group(GroupFrame {
            tag_keys,
            partition_key_vals,
        })) => format!(
            "GroupFrame, tag_keys: {}, partition_key_vals: {}",
            dump_u8_vec(tag_keys),
            dump_u8_vec(partition_key_vals),
        ),
        None => "<NO data field>".into(),
        _ => ":thinking_face: unknown frame type".into(),
    }
}

fn dump_values<T>(v: &[T]) -> String
where
    T: std::fmt::Display,
{
    v.iter()
        .map(|item| format!("{}", item))
        .collect::<Vec<_>>()
        .join(",")
}

fn dump_u8_vec(encoded_strings: &[Vec<u8>]) -> String {
    encoded_strings
        .iter()
        .map(|b| String::from_utf8_lossy(b))
        .collect::<Vec<_>>()
        .join(",")
}

fn dump_tags(tags: &[Tag]) -> String {
    tags.iter()
        .map(|tag| {
            format!(
                "{}={}",
                String::from_utf8_lossy(&tag.key),
                String::from_utf8_lossy(&tag.value),
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}
