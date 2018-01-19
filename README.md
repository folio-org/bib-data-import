# bib-data-import

Copyright (C) 2018 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

## Introduction
A Perl script to load binary MARC records into a FOLIO system using the [test-data-loader](https://github.com/folio-org/test-data-loader) module, including a reasonably full-fledged conversion rules file.

## Requirements
* FOLIO [test-data-loader](https://github.com/folio-org/test-data-loader) module
* FOLIO [mod-inventory-storage](https://github.com/folio-org/mod-inventory-storage) module

### Perl dependencies
* [DBI](https://metacpan.org/release/DBI)
* [DBD:Pg](https://metacpan.org/release/DBD-Pg)
* [LWP](https://metacpan.org/release/libwww-perl)
* [JSON](https://metacpan.org/release/JSON)

## Usage
**perl import.pl** \[**-h**\] **-t** *tenant\_id* **-u**
*storage\_URL* \[**-f** *number\_of\_processes*\]
\[**-r** *rules\_file*\] \[**--drop-indexes**\] \[**--analyze**\]
\[**--db-credentials** *db\_credentials\_file*\] *import\_directory* \[*import\_directory*... \] *data\_loader\_URL*

### Options
**-h** : Print help message.

**-t** : The tenant ID under which to load the records (required).

**-u** : The *storageURL* parameter to pass to the test-data-loader -- URL for mod-inventory-storage (required).

**-f** : Number of processes to fork for running the import in parallel (optional, default no forking).

**-r** : Path to a JSON file of conversion rules to post to the test-data-loader (optional, default `rules.json` in current working directory).

**--analyze** : Perform a `VACUUM ANALYZE` of the mod-inventory-storage `instance` table after data load (optional, if used requires **--db-credentials** option).

**--drop-indexes** : Drop indexes before performing data load, recreate after (optional, if used requires **--db-credentials** option). *WARNING*: Do not use this option on a production database, as it will severely affect search and sort performance on the `instance` table!

**--db-credentials** : Path to a file containing database credentials (optional, but required for **--drop-indexes** or **--analyze** options).

### Arguments
*import\_directory* : Path to a directory containing MARC binary files (in UTF-8 format). All files in the directory will be loaded. It is recommended that individual files contain no more than 50,000 records.

*data\_loader\_url* : URL for the test-data-loader.

### Database credentials
The *db\_credentials\_file* is a simple JSON file using the following format:

```json
{
  "host": "postgres.example.com",
  "port": 5432,
  "username": "folio_user",
  "password": "mysecretpassword",
  "database": "inventory_storage_database"
}
```

The values, of course, depend on your particular installation.
