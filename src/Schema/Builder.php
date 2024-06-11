<?php

declare(strict_types=1);

namespace LaraCassandra\Schema;

use Closure;
use RuntimeException;

use Illuminate\Database\Schema\Builder as BaseBuilder;
use Illuminate\Database\Connection as BaseConnection;
use Illuminate\Database\Schema\Blueprint as BaseBlueprint;

use LaraCassandra\Connection;
use LaraCassandra\Consistency;

class Builder extends BaseBuilder {
    protected ?Consistency $consistency = null;

    protected bool $ignoreWarnings = false;

    /**
     * @inheritdoc
     */
    public function __construct(BaseConnection $connection) {
        $this->connection = $connection;
        $this->grammar = $connection->getSchemaGrammar();

        $this->blueprintResolver(function (string $table, Closure $callback = null, string $prefix = '') {
            return new Blueprint($table, $callback, $prefix);
        });
    }

    /**
     * Create a keyspace in the schema.
     *
     * @param  string  $name
     * @param  ?array<string,mixed>  $replication
     * @return bool
     */
    public function createKeyspace($name, $replication = null) {

        if (!$this->grammar instanceof Grammar) {
            throw new RuntimeException('Invalid grammar selected.');
        }

        return $this->connection->statement(
            $this->grammar->compileCreateKeyspace($name, $this->connection, $replication)
        );
    }

    /**
     * Create a keyspace in the schema if it does not exist.
     *
     * @param  string  $name
     * @param  ?array<string,mixed>  $replication
     * @return bool
     */
    public function createKeyspaceIfNotExists($name, $replication = null) {

        if (!$this->grammar instanceof Grammar) {
            throw new RuntimeException('Invalid grammar selected.');
        }

        return $this->connection->statement(
            $this->grammar->compileCreateKeyspace($name, $this->connection, $replication, true)
        );
    }

    /**
     * Drop a keyspace from the schema if the keyspace exists.
     *
     * @param  string  $name
     * @return bool
     */
    public function dropKeyspaceIfExists($name) {

        if (!$this->grammar instanceof Grammar) {
            throw new RuntimeException('Invalid grammar selected.');
        }

        return $this->connection->statement(
            $this->grammar->compileDropKeyspaceIfExists($name)
        );
    }

    /**
     * Get the columns for a given table.
     *
     * @param  string  $table
     * @return array<mixed>
     */
    public function getColumns($table) {
        $table = $this->connection->getTablePrefix() . $table;

        if (!$this->connection instanceof Connection) {
            throw new RuntimeException('Invalid connection selected.');
        }

        if (!$this->grammar instanceof Grammar) {
            throw new RuntimeException('Invalid grammar selected.');
        }

        $results = $this->connection->selectFromWriteConnection(
            $this->grammar->compileColumns($this->connection->getKeyspaceName(), $table)
        );

        return $this->connection->getPostProcessor()->processColumns($results);
    }

    /**
     * Get the foreign keys for a given table.
     *
     * @param  string  $table
     * @return array<mixed>
     */
    public function getForeignKeys($table) {
        throw new RuntimeException('This database engine does not support foreign keys.');
    }

    /**
     * Get the indexes for a given table.
     *
     * @param  string  $table
     * @return array<mixed>
     */
    public function getIndexes($table) {
        $table = $this->connection->getTablePrefix() . $table;

        if (!$this->connection instanceof Connection) {
            throw new RuntimeException('Invalid connection selected.');
        }

        if (!$this->grammar instanceof Grammar) {
            throw new RuntimeException('Invalid grammar selected.');
        }

        return $this->connection->getPostProcessor()->processIndexes(
            $this->connection->selectFromWriteConnection(
                $this->grammar->compileIndexes($this->connection->getKeyspaceName(), $table)
            )
        );
    }

    /**
     * Get the tables for the database.
     *
     * @return array<mixed>
     */
    public function getTables() {

        if (!$this->connection instanceof Connection) {
            throw new RuntimeException('Invalid connection selected.');
        }

        if (!$this->grammar instanceof Grammar) {
            throw new RuntimeException('Invalid grammar selected.');
        }

        return $this->connection->getPostProcessor()->processTables(
            $this->connection->selectFromWriteConnection(
                $this->grammar->compileTables($this->connection->getKeyspaceName())
            )
        );
    }

    /**
     * Get the views for the database.
     *
     * @return array<mixed>
     */
    public function getViews() {
        if (!$this->connection instanceof Connection) {
            throw new RuntimeException('Invalid connection selected.');
        }

        if (!$this->grammar instanceof Grammar) {
            throw new RuntimeException('Invalid grammar selected.');
        }

        return $this->connection->getPostProcessor()->processViews(
            $this->connection->selectFromWriteConnection(
                $this->grammar->compileViews($this->connection->getKeyspaceName())
            )
        );
    }

    public function ignoreWarnings(bool $ignoreWarnings = true): self {
        $this->ignoreWarnings = $ignoreWarnings;

        return $this;
    }

    /**
     * Rename a table on the schema.
     *
     * @param  string  $from
     * @param  string  $to
     * @return void
     */
    public function rename($from, $to) {
        throw new RuntimeException('This database engine does not support renaming tables.');
    }

    public function setConsistency(Consistency $level): self {
        $this->consistency = $level;

        return $this;
    }

    protected function applyConsistency(): void {

        if (!$this->connection instanceof Connection) {
            throw new RuntimeException('Invalid connection selected.');
        }

        if ($this->consistency) {
            $this->connection->setConsistency($this->consistency);
        } else {
            $this->connection->setDefaultConsistency();
        }
    }

    protected function applyIgnoreWarnings(): void {

        if (!$this->connection instanceof Connection) {
            throw new RuntimeException('Invalid connection selected.');
        }

        if ($this->ignoreWarnings) {
            $this->connection->ignoreWarnings();
        } else {
            $this->connection->logWarnings();
        }
    }

    protected function build(BaseBlueprint $blueprint) {
        $this->applyConsistency();
        $this->applyIgnoreWarnings();
        parent::build($blueprint);
    }

}
