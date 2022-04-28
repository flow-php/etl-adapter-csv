<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\CSV\League\CSVExtractor;
use Flow\ETL\Adapter\CSV\League\CSVLoader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;

class CSV
{
    /**
     * @throws InvalidArgumentException
     */
    final public static function read_file(
        string $path,
        int $rows_in_batch = 1000,
        ?int $header_offset = null,
        string $operation_mode = 'r',
        string $rowEntry_name = 'row',
        string $delimiter = ',',
        string $enclosure = '"',
        string $escape = '\\'
    ) : Extractor {
        if (!\class_exists('League\Csv\Reader')) {
            throw new InvalidArgumentException("Missing League CSV dependency, please run 'composer require league/csv'");
        }

        if (!\file_exists($path)) {
            throw new InvalidArgumentException("File {$path} not found.'");
        }

        return new CSVExtractor(
            $path,
            $rows_in_batch,
            $header_offset,
            $operation_mode,
            $rowEntry_name,
            $delimiter,
            $enclosure,
            $escape
        );
    }

    /**
     * @throws InvalidArgumentException
     */
    final public static function read_directory(
        string $path,
        int $rows_in_batch = 1000,
        ?int $header_offset = null,
        string $operation_mode = 'r',
        string $rowEntry_name = 'row',
        string $delimiter = ',',
        string $enclosure = '"',
        string $escape = '\\'
    ) : Extractor {
        if (!\class_exists('League\Csv\Reader')) {
            throw new InvalidArgumentException("Missing League CSV dependency, please run 'composer require league/csv'");
        }

        if (!\file_exists($path) || !\is_dir($path)) {
            throw new InvalidArgumentException("Directory {$path} not found.'");
        }

        $directoryIterator = new \RecursiveDirectoryIterator($path);
        $directoryIterator->setFlags(\RecursiveDirectoryIterator::SKIP_DOTS);

        $regexIterator = new \RegexIterator(
            new \RecursiveIteratorIterator($directoryIterator),
            '/^.+\.csv$/i',
            \RecursiveRegexIterator::GET_MATCH
        );

        $extractors = [];

        /** @var array<string> $filePath */
        foreach ($regexIterator as $filePath) {
            $extractors[] = new CSVExtractor(
                /** @phpstan-ignore-next-line */
                \current($filePath),
                $rows_in_batch,
                $header_offset,
                $operation_mode,
                $rowEntry_name,
                $delimiter,
                $enclosure,
                $escape
            );
        }

        return new Extractor\ChainExtractor(...$extractors);
    }

    /**
     * @param string $path
     * @param string $open_mode
     * @param bool $safe_mode - when true path will become a folder and loader will write a csv file with a random name. Required while async processing.
     * @param bool $with_header
     * @param string $delimiter
     * @param string $enclosure
     * @param string $escape
     *
     * @throws InvalidArgumentException
     *
     * @return Loader
     */
    final public static function write(
        string $path,
        string $open_mode = 'w+',
        bool $safe_mode = true,
        bool $with_header = true,
        string $delimiter = ',',
        string $enclosure = '"',
        string $escape = '\\'
    ) : Loader {
        if (!\class_exists('League\Csv\Reader')) {
            throw new InvalidArgumentException("Missing League CSV dependency, please run 'composer require league/csv'");
        }

        return new CSVLoader($path, $open_mode, $safe_mode, $with_header, $delimiter, $enclosure, $escape);
    }
}
