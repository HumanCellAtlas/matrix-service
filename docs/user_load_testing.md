# User load testing

In order to prepare for Closed Beta and General Availability, the Matrix Service is in need of a solution to profile its
performance in terms of real world usage. A user load testing framework that can measure performance across a number of
the service's control inputs can achieve this. The following provides concrete definitions for the terms _performance_ 
and _control inputs_:

### Performance

Performance, in context of the User Load Tests, refers to the processing time of a matrix request measured from the
beginning of a POST request to the /matrix endpoint until the completion of the processing of the matrix request. We
are interested in measuring full processing times (i.e. cache misses). Cache misses will be tested by submitting a
random set of bundle UUIDs of the requested input size for each request. These times will be measured server side, i.e.
collection of the metrics will be implemented directly in the service code.

### Control Inputs
In order to provide a holistic picture of the service's performance, it is necessary to capture the full range of the
service's inputs in the tests run and results produced. The following defines control inputs of a single matrix request,
that are to be captured by the User Load Tests, including their respective ranges of values.

| Control Input | Description | Range of Values |
|---------------|-------------|-----------------|
| Input size | The size of the list of input bundles specified by either POST endpoint parameter `bundle_fqids` or `bundle_fqids_url`. This is the main input that controls the amount of processing a request needs to do and directly scales with processing times. | 1 - 10,000 + |
| Output format | The requested output format of the resultant expression matrix. This input affects processing times when a format requiring an AWS Batch conversion job (any other than zarr) is requested. This input may increase processing times due to Batch cold starts and/or resource constraints. | zarr, loom, mtx, csv, ... |
| Assay type | The type of assay performed to generate a specific bundle. With respect to a matrix request, this input should be named "Distribution of assay types across input bundles", however these tests will test a single assay type per request. A notable difference between SS2 and 10x types is that the former guarantees 1 cell per bundle whereas the latter may have thousands of cells per bundle. | SS2, 10x, ...|

These inputs need to be controlled as well as the main input of these tests, the **number of concurrent users/requests**.

## Proposal

Below is a proposal for a user load testing framework that covers how load tests will be implemented, what tests will be
run and how will test results be reported and shared.

### Requirements

- The testing framework should be able to simulate up to 100 concurrent users
- Tests must measure processing times of a matrix request from beginning (POST request) to end (generated resultant expression matrix in S3)
- Every test result must include data describing each control input
- Test results should produce and store performance benchmarks per environment
- Test results should be visualized
- Test results should be relevant to and compliment existing metrics (e.g. CloudWatch, Grafana)

### Framework

#### Running tests 

Use [Locust](https://locust.io/), an open-source user load testing framework, to run tests simulating concurrent
users/requests against the Matrix Service API. While Locust does provide tools to collect performance metrics (such as
HTTP response times), they are available only as aggregations (e.g. average response time) and are insufficient for our
purposes. For this reason, Locust will only be used for simulating the behavior of multiple users concurrently
executing matrix requests.

#### Capturing metrics

Instead, metrics will be captured by the Matrix Service and tracked as custom CloudWatch Metrics. The following
single [multi-dimensional metric](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/publishingMetrics.html)
will be captured during execution of a matrix request:

- Processing time (dimensions listed)
    - Input size : int
    - Output format: zarr | loom | mtx | csv
    - Assay type: SS2 | 10x
    
This metric and its dimensions cover the required control inputs.
    
#### Visualizing results

Using the above metrics, produce the following graphs in Grafana:

| Graph | Description | Control Inputs Captured |
|-------|-------------|-------------------------|
| Comparing Average Processing Times across Output Formats | A single graph that contains a plot of the average processing times for each output format. | Output format, Assay type |
| Comparing Input Size per Output Format | One graph per output format that contains a plot of the actual processing times for each input size bucket (e.g. 1-200 bundles, 201-400 bundles) | Input size, Output format, Assay type |

On completion of a Locust (user load) test, use the Grafana API to [retrieve a snapshot](http://docs.grafana.org/reference/export_import/)
of the relevant graphs at the time of test completion and upload these graphs to S3. One S3 bucket per deployment environment
will be created to store test results. Each test will correspond to a key in the bucket and will contain:
- links to the (interactive) snapshots of relevant graphs from Grafana
- metadata about the test (e.g. number of users, commit hash of deployment/matrix version)

#### Benchmarking

The ability to deterministically compare and rank test runs may enable us to iterate and optimize the performance more
effectively. For example, a top performance benchmark could be stored per deployment environment. More generally, however,
solving this problem may lead to tracking more meaningful metrics. This may be a good question to keep in mind moving
forward.
