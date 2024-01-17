<?php

namespace LaraCassandra;

use Cassandra\Exception as Exception;

class CassandraHelper {
    public function transformCql($query) {

        // Extracting the table name part from the query for SELECT, UPDATE, and INSERT
        preg_match("/from\s+([a-zA-Z0-9_\.]+)|update\s+([a-zA-Z0-9_\.]+)|insert\s+into\s+([a-zA-Z0-9_\.]+)/i", $query, $fromMatches);

        if (count($fromMatches) < 2) {
            throw new Exception('Unable to find table name in the query int CassandraHelper::transformCql');
        }

        // Extracting the full table name (might be with or without database name)
        $fullTableName = $fromMatches[1] ?: ($fromMatches[2] ?: $fromMatches[3]);

        // Splitting to get the actual table name
        $tableNameParts = explode('.', $fullTableName);
        $actualTableName = end($tableNameParts); // The table name is the last part after the dot

        // Checking if table name is mentioned with column names
        if (preg_match("/where\s+.*?\b$actualTableName\.|set\s+.*?\b$actualTableName\.|insert\s+into\s+$actualTableName\s+\((.*?)\)/i", $query, $columnMatches)) {
            if (!empty($columnMatches[1])) {
                // For INSERT, replace "tableName.columnName" in the column list
                $transformedQuery = preg_replace("/\b$actualTableName\.(\w+)/i", '$1', $query);
            } else {
                // For SELECT and UPDATE, replace "tableName.columnName" with "columnName"
                $transformedQuery = preg_replace("/\b$actualTableName\.(\w+)/i", '$1', $query);
            }
        } else {
            // If table name is not used with column names, return the original query
            $transformedQuery = $query;
        }

        return $transformedQuery;
    }
}
