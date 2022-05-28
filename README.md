# ETL Adapter: CSV

[![Minimum PHP Version](https://img.shields.io/badge/php-~8.1-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides Loaders and Extractors that works with CSV files.
Loader does not require any external dependencies, it's working on internal PHP functions.

## Installation 

``` 
composer require flow-php/etl-adapter-csv
```

## Extractor 

```php
<?php

use Flow\ETL\DSL\CSV;
use Flow\ETL\Flow;

$rows = (new Flow())
    ->read(CSV::from(new LocalFile($path)))
    ->fetch();
```

## Loader 

```php 
<?php

use Flow\ETL\DSL\CSV;
use Flow\ETL\Row;
use Flow\ETL\Rows;

(new Flow())
    ->process(
        new Rows(
            Row::create(new Row\Entry\IntegerEntry('id', 1), new Row\Entry\StringEntry('name', 'Norbert')),
            Row::create(new Row\Entry\IntegerEntry('id', 2), new Row\Entry\StringEntry('name', 'Tomek')),
            Row::create(new Row\Entry\IntegerEntry('id', 3), new Row\Entry\StringEntry('name', 'Dawid')),
        )
    )
    ->load(CSV::to($path, true, true))
    ->run();
```

## Development

In order to install dependencies please, launch following commands:

```bash
composer install
```

## Run Tests

In order to execute full test suite, please launch following command:

```bash
composer build
```

It's recommended to use [pcov](https://pecl.php.net/package/pcov) for code coverage however you can also use
xdebug by setting `XDEBUG_MODE=coverage` env variable.
