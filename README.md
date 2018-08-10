## HCA Matrix Service

Sits in front of the Blue Box and serves expression matrices based on queries.


#### How to run
| Commands       | Description | Dependencies |
| --------       | ----------- | ------------ |
| `make init`    | Initialize terraform configurations files |
| `make install` | Install all dependencies |
| `make test`    | Run all unittests (Make sure to deploy all AWS fixtures before running the tests) | `make deploy` |
| `make secrets` | Get `terraform.tfvars` from AWS Secret Manager |
| `make data`    | Get all performance data of matrix service async lambda function from S3 |
| `make clean-data`     | Clean downloaded performance data |
| `make upload-secrets` | Upload the configured terraform variables values into AWS Secret Manager (You need to fill the `CONFIG_TEMPLATE` defined in `scripts/upload-project-secrets.py`) | 
| `make build`          | Build the project and compress it into a zip file | `make install` |
| `make deploy`         | Deploy the whole project | `make build` |
| `make clean`          | Displace the project and clean local directory |
| `make all`            | Install all dependencies, package and deploy the project, and run unittests |


#### Performance Data Explanation
After executing the command `make data`, there will be a `\data` directory created. Within `\data`, there are 4 
sub-directories, where each of them contains the performance data for different versions of application.

| Sub-directory | Description |
| ------------- | ----------- |
| `v1.0.0`      | Sequential download from DSS with different lambda function memory configuration. |
| `v1.0.1`      | Parallel download from DSS with 1024 MB as lambda function memory size |
| `v1.0.2`      | Parallel download from faked DSS with 1024 MB as lambda function memory size |
| `v1.0.3`      | Parallel download from DSS with different `batch_size` while calling `loompy.combine()` |

For better understanding of those data, take a look at `notebooks/performance_characteristics.ipynb`, which visualize the
data into several plots.