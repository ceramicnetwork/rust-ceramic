use object_store::aws::AmazonS3Builder;

/// Configuration for pipeline session.
pub struct Config {
    /// Endpoint of a Flight SQL server for the conclusion feed.
    pub flight_sql_endpoint: String,

    /// AWS S3 configuration
    pub aws_s3_builder: AmazonS3Builder,
}
