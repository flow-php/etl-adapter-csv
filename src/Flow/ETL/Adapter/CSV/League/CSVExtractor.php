<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\League;

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use League\Csv\Reader;

/**
 * @psalm-immutable
 */
final class CSVExtractor implements Extractor
{
    private string $path;

    private int $rowsInBatch;

    private ?int $headerOffset;

    private string $operationMode;

    private string $rowEntryName;

    private ?Reader $reader;

    public function __construct(
        string $path,
        int $rowsInBatch,
        int $headerOffset = null,
        string $operationMode = 'r',
        string $rowEntryName = 'row'
    ) {
        $this->path = $path;
        $this->rowsInBatch = $rowsInBatch;
        $this->operationMode = $operationMode;
        $this->rowEntryName = $rowEntryName;
        $this->reader = null;
        $this->headerOffset = $headerOffset;
    }

    public function extract() : \Generator
    {
        $rows = [];

        /**
         * @psalm-suppress ImpureMethodCall
         *
         * @var array $row
         */
        foreach ($this->reader()->getIterator() as $row) {
            $rows[] = Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, $row));

            if (\count($rows) >= $this->rowsInBatch) {
                yield new Rows(...$rows);

                $rows = [];
            }
        }

        if (\count($rows)) {
            yield new Rows(...$rows);
        }
    }

    private function reader() : Reader
    {
        if ($this->reader === null) {
            $this->reader = Reader::createFromPath($this->path, $this->operationMode);
            $this->reader->setHeaderOffset($this->headerOffset);
        }

        return $this->reader;
    }
}
