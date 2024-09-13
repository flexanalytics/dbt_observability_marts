# DBT Observability Unification Project

This project is designed to unify all source tables from the `dbt_observability` package, providing a comprehensive view of your observability data across different environments. It's perfect for data teams looking to consolidate observability metrics and improve data monitoring and analysis.

## Getting Started

To make use of this project, you'll need to integrate it with your existing dbt setup. Follow the steps below to get started.

### Prerequisites

Ensure you have dbt installed and your project set up. This project is an addition to existing dbt projects that require observability across multiple schemas.

### Installation

This project uses stable release tags for both [minor and patch releases](https://semver.org/)

- v1-stable will keep your project up to date with all 1.x.x minor releases and v1.0-stable will keep your project up to date with all 1.0.x patch releases

1. Add this package to your `packages.yml`:
```yaml
packages:
 - git: "https://github.com/flexanalytics/dbt_observability_marts.git"
    revision: v1-stable  # or specific version with 1.0.1
```
2. **Configure your dbt_project.yml:**

You need to add specific configurations to your `dbt_project.yml` file to specify the databases and schemas you want to include in your observability marts.

   ```yaml
  vars:
  "dbt_observability:objects":  # database: [schemas]
    "dev": ["observability_schema_one", "observability_schema_two"]
    "prod": ["observability_schema_three", "observability_schema_four"]
```

### Usage
After installation and configuration, run your dbt project as usual. The observability marts will be generated based on the sources specified in your dbt_project.yml file.

### Contributing
We welcome contributions to this project! Whether it's adding new features, improving documentation, or reporting bugs, please feel free to make a pull request or open an issue.

### License
This project is licensed under the Apache 2.0 License - see the LICENSE file for details.
