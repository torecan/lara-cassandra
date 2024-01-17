<?php

namespace Torecan\LaraCasandra\Query;

use Illuminate\Database\Query\Processors\Processor as BaseProcessor;
use Illuminate\Database\Query\Builder;

class Processor extends BaseProcessor {
    public function processInsertGetId(Builder $query, $sql, $values, $sequence = null) {
        $query->getConnection()->insert($sql, $values);

        return null;
    }

    /**
     * Process the results of a tables query.
     *
     * @param array $results
     * @return array
     */
    public function processTables($results) {
        return [];
    }
}
