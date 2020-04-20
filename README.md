# Union input plugin for Embulk
![Release CI Status Badge](https://github.com/civitaspo/embulk-output-s3_parquet/workflows/Release%20CI/badge.svg) ![Test CI Status Badge](https://github.com/civitaspo/embulk-output-s3_parquet/workflows/Test%20CI/badge.svg)

An [embulk](https://github.com/embulk/embulk/) input plugin to union all loaded data.

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: yes
* **Guess supported**: no

## Configuration

- **union**: Embulk configurations for input data. (array of config, required)
  - **name**: The name of the bulk load. (string, optional)
  - **exec**: The embulk execution configuration. (config, optional)
    - **max_threads**: Maximum number of threads to run concurrently. (int, default: 2x of available CPU cores) 
  - **in**: The embulk input plugin configuration. (config, required)
  - **filters**: The embulk filter plugin configurations. (array of config, default: `[]`)

## Example

```yaml
in:
  type: union
  union:
    - in:
        type: file
        path_prefix: ./example/data01.tsv
        parser:
          type: csv
          delimiter: "\t"
          skip_header_lines: 0
          null_string: ""
          columns:
            - { name: id, type: long }
            - { name: description, type: string }
            - { name: name, type: string }
            - { name: t, type: timestamp, format: "%Y-%m-%d %H:%M:%S %z" }
            - { name: payload, type: json }
          stop_on_invalid_record: true
      filters:
        - type: column
          add_columns:
            - { name: group_name, type: string, default: "data01" }
    - in:
        type: file
        path_prefix: ./example/data02.tsv
        parser:
          type: csv
          delimiter: "\t"
          skip_header_lines: 0
          null_string: ""
          columns:
            - { name: id, type: long }
            - { name: description, type: string }
            - { name: name, type: string }
            - { name: t, type: timestamp, format: "%Y-%m-%d %H:%M:%S %z" }
            - { name: payload, type: json }
          stop_on_invalid_record: true
      filters:
        - type: column
          add_columns:
            - { name: group_name, type: string, default: "data02" }

out:
  type: stdout
```

## Development

### Run examples

```shell
$ ./gradlew gem
$ embulk bundle install --gemfile ./example/Gemfile
$ embulk run example/config.yml -I build/gemContents/lib -b example
```

### Run tests

```shell
$ ./gradlew test
```

### Run the formatter

```shell
## Just check the format violations
$ ./gradlew spotlessCheck

## Fix the all format violations
$ ./gradlew spotlessApply
```

### Build

```shell
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

### Release a new gem

```shell
$ ./gradlew gemPush
```

## CHANGELOG

[CHANGELOG.md](./CHANGELOG.md)

## License

[MIT LICENSE](./LICENSE)
