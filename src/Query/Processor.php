<?php

declare(strict_types=1);

namespace LaraCassandra\Query;

use Illuminate\Database\Query\Processors\Processor as BaseProcessor;
use Illuminate\Database\Query\Builder;

use Cassandra\Exception as CassandraException;

class Processor extends BaseProcessor {
    /**
     * Process an  "insert get ID" query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  string  $sql
     * @param  array<mixed>  $values
     * @param  string|null  $sequence
     * @return int
     */
    public function processInsertGetId(Builder $query, $sql, $values, $sequence = null) {
        throw new CassandraException('"Insert get ID" is not supported by the database.');
    }
}
