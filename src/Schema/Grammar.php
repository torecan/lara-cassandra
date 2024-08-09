<?php

declare(strict_types=1);

namespace LaraCassandra\Schema;

use RuntimeException;

use Illuminate\Database\Schema\Grammars\Grammar as BaseGrammar;
use Illuminate\Support\Fluent;
use Illuminate\Database\Connection as BaseConnection;
use Illuminate\Database\Schema\Blueprint as BaseBlueprint;

class Grammar extends BaseGrammar {
    protected string $keyspaceName = '';

    /**
     * The possible column modifiers.
     *
     * @var array<string>
     */
    protected $modifiers = [
        'PrimaryKey', 'Static', 'Nullable',
    ];

    /**
     * Compile an add column command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent<string,mixed>  $command
     * @return string
     */
    public function compileAdd(BaseBlueprint $blueprint, Fluent $command) {
        $columns = $this->prefixArray('add', $this->getColumns($blueprint));

        return 'alter table ' . $this->wrapTable($blueprint) . ' ' . implode(', ', $columns);
    }
    /**
     * Compile a change column command into a series of SQL statements.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent<string,mixed>  $command
     * @param  \Illuminate\Database\Connection  $connection
     * @return array<mixed>|string
     *
     * @throws \RuntimeException
     */
    public function compileChange(BaseBlueprint $blueprint, Fluent $command, BaseConnection $connection) {
        $changes = [];

        foreach ($blueprint->getChangedColumns() as $column) {
            $sql = sprintf('alter table %s alter column %s %s',
                $this->wrapTable($blueprint),
                $this->wrap($column),
                $this->getType($column)
            );

            foreach ($this->modifiers as $modifier) {
                if (method_exists($this, $method = "modify{$modifier}")) {
                    $sql .= $this->{$method}($blueprint, $column);
                }
            }

            $changes[] = $sql;
        }

        return $changes;
    }

    /**
     * Compile the query to determine the columns.
     *
     * @param  string  $keyspace
     * @param  string  $table
     * @return string
     */
    public function compileColumns($keyspace, $table) {

        return sprintf(
            'select column_name as name, type, '
            . 'kind, clustering_order, position, '
            . 'from system_schema.columns where keyspace_name = %s and table_name = %s '
            . 'order by column_name',
            $this->quoteString($keyspace),
            $this->quoteString($table)
        );
    }

    /**
     * Compile a create table command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent<string,mixed>  $command
     * @param  \Illuminate\Database\Connection  $connection
     * @return string
     */
    public function compileCreate(BaseBlueprint $blueprint, Fluent $command, BaseConnection $connection) {
        $sql = $this->compileCreateTable(
            $blueprint, $command, $connection
        );

        return $sql;
    }

    /**
     * Compile a create keyspace command.
     *
     * @param  ?array<string,mixed>  $replication
     * @param  \Illuminate\Database\Connection  $connection
     * @return string
     */
    public function compileCreateKeyspace(string $name, BaseConnection $connection, ?array $replication = null, bool $ifNotExists = false) {

        $replication ??= $connection->getConfig('default_replication') ?? [
            'class' => 'SimpleStrategy',
            'replication_factor' => 1,
        ];

        if (!is_array($replication)) {
            throw new RuntimeException('replication config must be an array.');
        }

        $replicationOptions = [];

        foreach ($replication as $key => $value) {
            $replicationOptions[] = "'{$key}': {$value}";
        }

        return sprintf(
            'create keyspace %s%s with replication = {%s}',
            $ifNotExists ? 'if not exists ' : '',
            $this->wrapValue($name),
            implode(', ', $replicationOptions)
        );
    }

    /**
     * Compile the command to disable foreign key constraints.
     *
     * @return string
     */
    public function compileDisableForeignKeyConstraints() {
        throw new RuntimeException('This database driver does not support foreign key creation.');
    }

    /**
       * Compile a drop table command.
       *
       * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
       * @param  \Illuminate\Support\Fluent<string,mixed>  $command
       * @return string
       */
    public function compileDrop(BaseBlueprint $blueprint, Fluent $command) {
        return 'drop table ' . $this->wrapTable($blueprint);
    }

    /**
     * Compile a drop column command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent<string,mixed>  $command
     * @return string
     */
    public function compileDropColumn(BaseBlueprint $blueprint, Fluent $command) {

        $columnsInfo = $command->value('columns');
        if (!is_array($columnsInfo)) {
            throw new RuntimeException('Columns must be an array.');
        }

        $columns = $this->prefixArray('drop', $this->wrapArray($columnsInfo));

        return 'alter table ' . $this->wrapTable($blueprint) . ' ' . implode(', ', $columns);
    }

    /**
     * Compile a drop table (if exists) command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent<string,mixed>  $command
     * @return string
     */
    public function compileDropIfExists(BaseBlueprint $blueprint, Fluent $command) {
        return 'drop table if exists ' . $this->wrapTable($blueprint);
    }

    /**
     * Compile a drop index command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent<string,mixed>  $command
     * @return string
     */
    public function compileDropIndex(BaseBlueprint $blueprint, Fluent $command) {

        $indexInfo = $command->value('index');
        if (!is_string($indexInfo)) {
            throw new RuntimeException('Index must be a string.');
        }

        $index = $this->wrap($blueprint->getTable() . '_' . $indexInfo . '_index');

        return "drop index {$index}";
    }

    /**
     * Compile a drop keyspace if exists command.
     *
     * @param  string  $name
     * @return string
     */
    public function compileDropKeyspaceIfExists($name) {
        return sprintf(
            'drop keyspace if exists %s',
            $this->wrapValue($name)
        );
    }

    /**
     * Compile the command to enable foreign key constraints.
     *
     * @return string
     */
    public function compileEnableForeignKeyConstraints() {
        throw new RuntimeException('This database driver does not support foreign key creation.');
    }

    /**
     * Compile a foreign key command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent<string,mixed>  $command
     * @return string
     */
    public function compileForeign(BaseBlueprint $blueprint, Fluent $command) {
        throw new RuntimeException('This database driver does not support foreign key creation.');
    }

    /**
     * Compile a plain index key command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent<string,mixed>  $command
     * @return string
     */
    public function compileIndex(BaseBlueprint $blueprint, Fluent $command) {

        $indexInfo = $command->value('index');
        if (!is_string($indexInfo)) {
            throw new RuntimeException('Index must be a string.');
        }

        $columnsInfo = $command->value('columns');
        if (!is_array($columnsInfo)) {
            throw new RuntimeException('Columns must be an array.');
        }

        return sprintf('create index %s on %s (%s)',
            $this->wrap($indexInfo),
            $this->wrapTable($blueprint),
            $this->columnize($columnsInfo)
        );
    }

    /**
     * Compile the query to determine the indexes.
     *
     * @param  string  $keyspace
     * @param  string  $table
     * @return string
     */
    public function compileIndexes($keyspace, $table) {
        return sprintf(
            'select index_name as name, kind as type, options '
            . 'from system_schema.indexes where keyspace_name = %s and table_name = %s',
            $this->quoteString($keyspace),
            $this->quoteString($table)
        );
    }

    /**
     * Compile the query to determine the keyspaces.
     *
     * @return string
     */
    public function compileKeyspaces() {
        return sprintf(
            'select keyspace_name as name, replication, durable_writes '
            . 'from system_schema.keyspaces'
        );
    }

    /**
     * Compile a primary key command.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent<string,mixed>  $command
     * @return string
     */
    public function compilePrimary(BaseBlueprint $blueprint, Fluent $command) {
        throw new RuntimeException('This database driver does not support primary key creation.');
    }

    /**
     * Compile the query to determine the tables.
     *
     * @param  string  $keyspace
     * @return string
     */
    public function compileTables($keyspace) {
        return sprintf(
            'select table_name as name, comment '
            . 'from system_schema.tables where keyspace_name = %s '
            . 'order by table_name',
            $this->quoteString($keyspace)
        );
    }

    /**
     * Compile the query to determine the views.
     *
     * @param  string  $keyspace
     * @return string
     */
    public function compileViews($keyspace) {
        return sprintf(
            'select view_name as name, base_table_name, where_clause, comment '
            . 'from system_schema.views where keyspace_name = %s',
            $this->quoteString($keyspace)
        );
    }

    public function getKeyspaceName(): string {
        return $this->keyspaceName;
    }

    public function setKeyspaceName(string $keyspaceName): void {
        $this->keyspaceName = $keyspaceName;
    }

    /**
     * Wrap a table in keyword identifiers.
     *
     * @param  mixed  $table
     * @return string
     */
    public function wrapTable($table) {
        $table = parent::wrapTable($table);

        $keyspaceName = $this->getKeyspaceName();
        if ($keyspaceName) {
            $table = $this->wrapValue($keyspaceName) . '.' . $table;
        }

        return $table;
    }

    /**
     * Create the main create table clause.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent<string,mixed>  $command
     * @param  \Illuminate\Database\Connection  $connection
     * @return string
     */
    protected function compileCreateTable($blueprint, $command, $connection) {
        $tableStructure = $this->getColumns($blueprint);

        // partion key(s)
        $partitionKeys = $this->getCommandsByName($blueprint, 'partition');
        $partitionKeyColumns = [];
        foreach ($partitionKeys as $partitionKey) {
            $columns = $partitionKey->value('columns');
            if (!is_array($columns)) {
                throw new RuntimeException('Partition key columns must be an array.');
            }

            $partitionKey->offsetSet('shouldBeSkipped', true);

            $partitionKeyColumns = array_merge($partitionKeyColumns, $columns);
        }
        $partitionKeyCql = $this->columnize($partitionKeyColumns);

        if (!$partitionKeyCql) {
            throw new RuntimeException('Partition key must be defined.');
        }

        // clustering key(s)
        $clusteringKeys = $this->getCommandsByName($blueprint, 'clustering');
        $clusteringKeyColumns = [];
        foreach ($clusteringKeys as $clusteringKey) {
            $columns = $clusteringKey->value('columns');
            if (!is_array($columns)) {
                throw new RuntimeException('Partition key columns must be an array.');
            }

            $clusteringKey->offsetSet('shouldBeSkipped', true);

            foreach ($columns as $column) {
                $clusteringKeyColumns[$column] =  $clusteringKey->algorithm;
            }
        }
        $clusteringKeyCql = $this->columnize(array_keys($clusteringKeyColumns));

        if ($clusteringKeyCql) {
            $keyCql = '(' . $partitionKeyCql . '), ' . $clusteringKeyCql;

            $clusteringOrders = [];
            foreach ($clusteringKeyColumns as $name => $orderBy) {
                $clusteringOrders[] = $name . ' ' . $orderBy;
            }

            $clusteringOrderCql = ' WITH CLUSTERING ORDER BY (' . implode(', ', $clusteringOrders) . ')';
        } else {
            $keyCql = $partitionKeyCql;
            $clusteringOrderCql = '';
        }

        $tableStructure[] = sprintf(
            'primary key (%s)',
            $keyCql
        );

        return sprintf('%s table %s (%s)%s',
            'create',
            $this->wrapTable($blueprint),
            implode(', ', $tableStructure),
            $clusteringOrderCql
        );
    }

    /**
     * Get the SQL for a nullable column modifier.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent<string,mixed>  $column
     * @return string|null
     */
    protected function modifyNullable(BaseBlueprint $blueprint, Fluent $column) {

        return null;
    }

    /**
     * Get the SQL for an primary key column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string|null
     */
    protected function modifyPrimaryKey(BaseBlueprint $blueprint, Fluent $column) {
        if ($column->value('primaryKey')) {
            return ' primary key';
        }

        return null;
    }

    /**
     * Get the SQL for an static column modifier.
     *
     * @param \Illuminate\Database\Schema\Blueprint $blueprint
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string|null
     */
    protected function modifyStatic(BaseBlueprint $blueprint, Fluent $column) {
        if ($column->value('static')) {
            return ' static';
        }

        return null;
    }

    /**
     * Create the column definition for a boolean type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeAscii(Fluent $column) {
        return 'ascii';
    }

    /**
     * Create the column definition for a text type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeBigint(Fluent $column) {
        return 'bigint';
    }

    /**
     * Create the column definition for a blob type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeBlob(Fluent $column) {
        return 'blob';
    }

    /**
     * Create the column definition for a boolean type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeBoolean(Fluent $column) {
        return 'boolean';
    }

    /**
     * Create the column definition for a counter type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeCounter(Fluent $column) {
        return 'counter';
    }

    /**
     * Create the column definition for a date type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeDate(Fluent $column) {
        return 'date';
    }

    /**
     * Create the column definition for a decimal type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeDecimal(Fluent $column) {
        return 'decimal';
    }

    /**
      * Create the column definition for a double type.
      *
      * @param \Illuminate\Support\Fluent<string,mixed> $column
      * @return string
      */
    protected function typeDouble(Fluent $column) {
        return 'double';
    }

    /**
      * Create the column definition for a duration type.
      *
      * @param \Illuminate\Support\Fluent<string,mixed> $column
      * @return string
      */
    protected function typeDuration(Fluent $column) {
        return 'duration';
    }

    /**
      * Create the column definition for a float type.
      *
      * @param \Illuminate\Support\Fluent<string,mixed> $column
      * @return string
      */
    protected function typeFloat(Fluent $column) {
        return 'float';
    }

    /**
     * Create the column definition for a frozen type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeFrozen(Fluent $column) {
        return 'frozen';
    }

    /**
     * Create the column definition for a inet type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeInet(Fluent $column) {
        return 'inet';
    }

    /**
     * Create the column definition for a int type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeInt(Fluent $column) {
        return 'int';
    }

    /**
     * Create the column definition for a list type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeList(Fluent $column) {

        $collectionType = $column->value('collectionType');
        if (!is_string($collectionType)) {
            throw new RuntimeException('collectionType must be a string.');
        }

        return 'list<' . $collectionType . '>';
    }

    /**
     * Create the column definition for a map type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeMap(Fluent $column) {

        $collectionType1 = $column->value('collectionType1');
        if (!is_string($collectionType1)) {
            throw new RuntimeException('collectionType1 must be a string.');
        }

        $collectionType2 = $column->value('collectionType2');
        if (!is_string($collectionType2)) {
            throw new RuntimeException('collectionType2 must be a string.');
        }

        return 'map<' . $collectionType1 . ', ' . $collectionType2 . '>';
    }

    /**
     * Create the column definition for a set type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeSet(Fluent $column) {

        $collectionType = $column->value('collectionType');
        if (!is_string($collectionType)) {
            throw new RuntimeException('collectionType must be a string.');
        }

        return 'set<' . $collectionType . '>';
    }

    /**
     * Create the column definition for a small integer type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeSmallint(Fluent $column) {
        return 'smallint';
    }

    /**
     * Create the column definition for a text type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeText(Fluent $column) {
        return 'text';
    }

    /**
     * Create the column definition for a time type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeTime(Fluent $column) {
        return 'time';
    }

    /**
     * Create the column definition for a timestamp type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeTimestamp(Fluent $column) {
        return 'timestamp';
    }

    /**
     * Create the column definition for a timeuuid type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeTimeuuid(Fluent $column) {
        return 'timeuuid';
    }

    /**
     * Create the column definition for a tiny integer type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeTinyint(Fluent $column) {
        return 'tinyint';
    }

    /**
     * Create the column definition for a tuple type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeTuple(Fluent $column) {

        $tuple1type = $column->value('tuple1type');
        if (!is_string($tuple1type)) {
            throw new RuntimeException('tuple1type must be a string.');
        }

        $tuple2type = $column->value('tuple2type');
        if (!is_string($tuple2type)) {
            throw new RuntimeException('tuple2type must be a string.');
        }

        $tuple3type = $column->value('tuple3type');
        if (!is_string($tuple3type)) {
            throw new RuntimeException('tuple3type must be a string.');
        }

        return 'tuple<' . $tuple1type . ', ' . $tuple2type . ', ' . $tuple3type . '>';
    }

    /**
     * Create the column definition for a uuid type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeUuid(Fluent $column) {
        return 'uuid';
    }

    /**
     * Create the column definition for a varchar type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeVarchar(Fluent $column) {
        return 'varchar';
    }

    /**
     * Create the column definition for a varint type.
     *
     * @param \Illuminate\Support\Fluent<string,mixed> $column
     * @return string
     */
    protected function typeVarint(Fluent $column) {
        return 'varint';
    }

    /**
     * Wrap a single string in keyword identifiers.
     *
     * @param  string  $value
     * @return string
     */
    protected function wrapValue($value) {
        if ($value !== '*') {
            return '"' . str_replace('"', '""', $value) . '"';
        }

        return $value;
    }
}
