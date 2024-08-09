<?php

declare(strict_types=1);

namespace LaraCassandra\Schema;

use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint as BaseBlueprint;
use Illuminate\Database\Schema\Grammars\Grammar;
use RuntimeException;

class Blueprint extends BaseBlueprint {
    /**
     * Create a new ascii column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function ascii($column) {
        return $this->addColumn('ascii', $column);
    }

    /**
     * Create a new auto-incrementing big integer (8-byte) column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function bigIncrements($column) {
        throw new RuntimeException('This database driver does not support auto-increment columns.');
    }

    /**
     * Create a new bigint column on the table.
     * 
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function bigint($column) {
        return $this->addColumn('bigint', $column);
    }

    /**
     * Create a new big integer (8-byte) column on the table.
     *
     * @param  string  $column
     * @param  bool  $autoIncrement
     * @param  bool  $unsigned
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function bigInteger($column, $autoIncrement = false, $unsigned = false) {
        return $this->addColumn('bigint', $column);
    }

    /**
     * Create a new binary column on the table.
     *
     * @param  string  $column
     * @param  int|null  $length
     * @param  bool  $fixed
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function binary($column, $length = null, $fixed = false) {
        return $this->addColumn('blob', $column);
    }

    /**
     * Create a new blob column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function blob($column) {
        return $this->addColumn('blob', $column);
    }

    /**
     * Create a new boolean column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function boolean($column) {
        return $this->addColumn('boolean', $column);
    }

    /**
     * Create a new char column on the table.
     *
     * @param  string  $column
     * @param  int|null  $length
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function char($column, $length = null) {

        return $this->addColumn('varchar', $column);
    }

    /**
     * Specify the character set that should be used for the table.
     *
     * @param  string  $charset
     * @return void
     */
    public function charset($charset) {
        throw new RuntimeException('This database driver does not support setting the charset.');
    }

    /**
     * Specify the clustering key(s) for the table.
     *
     * @param  string|array<mixed>  $columns
     * @param  string|null  $orderBy
     * @param  string|null  $name
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function clustering($columns, $orderBy = null, $name = null) {

        if (!$orderBy) {
            $orderBy = 'ASC';
        } else {
            $orderBy = strtoupper($orderBy);
        }

        return $this->indexCommand('clustering', $columns, $name ?? '', $orderBy);
    }

    /**
     * Specify the collation that should be used for the table.
     *
     * @param  string  $collation
     * @return void
     */
    public function collation($collation) {
        throw new RuntimeException('This database driver does not support setting the collation.');
    }

    /**
     * Create a new counter column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function counter($column) {
        return $this->addColumn('counter', $column);
    }

    /**
     * Create a new date-time column on the table.
     *
     * @param  string  $column
     * @param  int|null  $precision
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function dateTime($column, $precision = 0) {
        return $this->addColumn('timestamp', $column);
    }

    /**
     * Create a new date-time column (with time zone) on the table.
     *
     * @param  string  $column
     * @param  int|null  $precision
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function dateTimeTz($column, $precision = 0) {
        return $this->addColumn('timestamp', $column);
    }

    /**
     * Create a new decimal column on the table.
     *
     * @param  string  $column
     * @param  int  $total
     * @param  int  $places
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function decimal($column, $total = 8, $places = 2) {
        return $this->addColumn('decimal', $column);
    }

    /**
     * Indicate that the given column and foreign key should be dropped.
     *
     * @param  string  $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function dropConstrainedForeignId($column) {
        throw new RuntimeException('This database driver does not support foreign keys.');
    }

    /**
     * Indicate that the given foreign key should be dropped.
     *
     * @param  \Illuminate\Database\Eloquent\Model|string  $model
     * @param  string|null  $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function dropConstrainedForeignIdFor($model, $column = null) {
        throw new RuntimeException('This database driver does not support foreign keys.');
    }

    /**
     * Indicate that the given foreign key should be dropped.
     *
     * @param  string|array<mixed>  $index
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function dropForeign($index) {
        throw new RuntimeException('This database driver does not support foreign keys.');
    }

    /**
     * Indicate that the given foreign key should be dropped.
     *
     * @param  \Illuminate\Database\Eloquent\Model|string  $model
     * @param  string|null  $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function dropForeignIdFor($model, $column = null) {
        throw new RuntimeException('This database driver does not support foreign keys.');
    }

    /**
     * Indicate that the given fulltext index should be dropped.
     *
     * @param  string|array<mixed>  $index
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function dropFullText($index) {
        throw new RuntimeException('This database driver does not support fulltext indexes.');
    }

    /**
     * Indicate that the given primary key should be dropped.
     *
     * @param  string|array<mixed>|null  $index
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function dropPrimary($index = null) {
        throw new RuntimeException('This database driver does not support dropping a primary index.');
    }

    /**
     * Indicate that the given spatial index should be dropped.
     *
     * @param  string|array<mixed>  $index
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function dropSpatialIndex($index) {
        throw new RuntimeException('This database driver does not support spatial indexes.');
    }

    /**
     * Indicate that the given unique key should be dropped.
     *
     * @param  string|array<mixed>  $index
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function dropUnique($index) {
        throw new RuntimeException('This database driver does not support unique indexes.');
    }

    /**
     * Create a new duration column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function duration($column) {
        return $this->addColumn('duration', $column);
    }

    /**
     * Specify the storage engine that should be used for the table.
     *
     * @param  string  $engine
     * @return void
     */
    public function engine($engine) {
        throw new RuntimeException('This database driver does not support setting the storage engine.');
    }

    /**
     * Create a new enum column on the table.
     *
     * @param  string  $column
     * @param  array<mixed>  $allowed
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function enum($column, array $allowed) {
        throw new RuntimeException('This database driver does not support the enum columns.');
    }

    /**
     * Create a new float column on the table.
     *
     * @param  string  $column
     * @param  int  $precision
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function float($column, $precision = 53) {
        return $this->addColumn('float', $column);
    }
    /**
     * Specify a foreign key for the table.
     *
     * @param  string|array<mixed>  $columns
     * @param  string|null  $name
     * @return \Illuminate\Database\Schema\ForeignKeyDefinition
     */
    public function foreign($columns, $name = null) {
        throw new RuntimeException('This database driver does not support foreign keys.');
    }

    /**
     * Create a new frozen column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function frozen($column) {
        return $this->addColumn('frozen', $column);
    }
    /**
     * Specify an fulltext for the table.
     *
     * @param  string|array<mixed>  $columns
     * @param  string|null  $name
     * @param  string|null  $algorithm
     * @return \Illuminate\Database\Schema\IndexDefinition
     */
    public function fullText($columns, $name = null, $algorithm = null) {
        throw new RuntimeException('This database driver does not support fulltext indexes.');
    }

    /**
     * Create a new geography column on the table.
     *
     * @param  string  $column
     * @param  string|null  $subtype
     * @param  int  $srid
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function geography($column, $subtype = null, $srid = 4326) {
        throw new RuntimeException('This database driver does not support geography columns.');
    }

    /**
     * Create a new geometry column on the table.
     *
     * @param  string  $column
     * @param  string|null  $subtype
     * @param  int  $srid
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function geometry($column, $subtype = null, $srid = 0) {
        throw new RuntimeException('This database driver does not support geometry columns.');
    }

    /**
     * Create a new auto-incrementing big integer (8-byte) column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function id($column = 'id') {
        throw new RuntimeException('This database driver does not support auto-increment columns.');
    }
    /**
     * Create a new auto-incrementing integer (4-byte) column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function increments($column) {
        throw new RuntimeException('This database driver does not support auto-increment columns.');
    }

    /**
     * Create a new inet column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function inet($column) {
        return $this->addColumn('inet', $column);
    }

    /**
     * Specify that the InnoDB storage engine should be used for the table (MySQL only).
     *
     * @return void
     */
    public function innoDb() {
        throw new RuntimeException('This database driver does not support setting the storage engine.');
    }

    /**
     * Create a new int column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function int($column) {
        return $this->addColumn('int', $column);
    }

    /**
     * Create a new integer (4-byte) column on the table.
     *
     * @param  string  $column
     * @param  bool  $autoIncrement
     * @param  bool  $unsigned
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function integer($column, $autoIncrement = false, $unsigned = false) {
        return $this->addColumn('int', $column);
    }

    /**
     * Create a new auto-incrementing integer (4-byte) column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function integerIncrements($column) {
        throw new RuntimeException('This database driver does not support auto-increment columns.');
    }

    /**
      * Create a new IP address column on the table.
      *
      * @param  string  $column
      * @return \Illuminate\Database\Schema\ColumnDefinition
      */
    public function ipAddress($column = 'ip_address') {
        throw new RuntimeException('This database driver does not support ip address columns.');
    }

    /**
     * Create a new json column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function json($column) {
        throw new RuntimeException('This database driver does not support json columns.');
    }

    /**
     * Create a new jsonb column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function jsonb($column) {
        throw new RuntimeException('This database driver does not support jsonb columns.');
    }

    /**
     * Create a new list column on the table.
     *
     * @param string $column
     * @param string $collectionType
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function listCollection($column, $collectionType) {
        return $this->addColumn('list', $column, compact('collectionType'));
    }

    /**
     * Create a new long text column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function longText($column) {
        return $this->addColumn('varchar', $column);
    }

    /**
     * Create a new map column on the table.
     *
     * @param string $column
     * @param string $collectionType1
     * @param string $collectionType2
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function mapCollection($column, $collectionType1, $collectionType2) {
        return $this->addColumn('map', $column, compact('collectionType1', 'collectionType2'));
    }

    /**
     * Create a new auto-incrementing medium integer (3-byte) column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function mediumIncrements($column) {
        throw new RuntimeException('This database driver does not support auto-increment columns.');
    }

    /**
     * Create a new medium integer (3-byte) column on the table.
     *
     * @param  string  $column
     * @param  bool  $autoIncrement
     * @param  bool  $unsigned
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function mediumInteger($column, $autoIncrement = false, $unsigned = false) {
        throw new RuntimeException('This database driver does not support medium integer (3-byte) columns.');
    }

    /**
     * Create a new medium text column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function mediumText($column) {
        return $this->addColumn('varchar', $column);
    }

    /**
     * Specify the parition key(s) for the table.
     *
     * @param  string|array<mixed>  $columns
     * @param  string|null  $name
     * @param  string|null  $algorithm
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function partition($columns, $name = null, $algorithm = null) {

        return $this->indexCommand('partition', $columns, $name ?? '', $algorithm);
    }

    /**
     * Specify the primary key(s) for the table.
     *
     * @param  string|array<mixed>  $columns
     * @param  string|null  $name
     * @param  string|null  $algorithm
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function primary($columns, $name = null, $algorithm = null) {
        return $this->indexCommand('partition', $columns, $name ?? '', $algorithm);
    }

    /**
     * Rename the table to a given name.
     *
     * @param  string  $to
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function rename($to) {
        throw new RuntimeException('This database driver does not support renaming tables.');
    }

    /**
     * Indicate that the given indexes should be renamed.
     *
     * @param  string  $from
     * @param  string  $to
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function renameIndex($from, $to) {
        throw new RuntimeException('This database driver does not renaming indexes.');
    }

    /**
     * Create a new set column on the table.
     *
     * @param string $column
     * @param string $collectionType
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function setCollection($column, $collectionType) {
        return $this->addColumn('set', $column, compact('collectionType'));
    }

    /**
     * Create a new auto-incrementing small integer (2-byte) column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function smallIncrements($column) {
        return $this->unsignedSmallInteger($column, true);
    }

    /**
     * Create a new smallint column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function smallint($column) {
        return $this->addColumn('smallint', $column);
    }

    /**
     * Create a new small integer (2-byte) column on the table.
     *
     * @param  string  $column
     * @param  bool  $autoIncrement
     * @param  bool  $unsigned
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function smallInteger($column, $autoIncrement = false, $unsigned = false) {
        return $this->addColumn('smallint', $column);
    }

    /**
     * Specify a spatial index for the table.
     *
     * @param  string|array<mixed>  $columns
     * @param  string|null  $name
     * @return \Illuminate\Database\Schema\IndexDefinition
     */
    public function spatialIndex($columns, $name = null) {
        throw new RuntimeException('This database driver does not support spatial indexes.');
    }

    /**
     * Create a new string column on the table.
     *
     * @param  string  $column
     * @param  int|null  $length
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function string($column, $length = null) {
        $length = $length ?: Builder::$defaultStringLength;

        return $this->addColumn('varchar', $column);
    }

    /**
     * Indicate that the table needs to be temporary.
     *
     * @return void
     */
    public function temporary() {
        throw new RuntimeException('This database driver does not support temporary tables.');
    }

    /**
     * Create a new time column on the table.
     *
     * @param  string  $column
     * @param  int|null  $precision
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function time($column, $precision = 0) {
        return $this->addColumn('time', $column);
    }

    /**
     * Create a new timestamp column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function timestamp($column, $precision = 0) {
        return $this->addColumn('timestamp', $column);
    }

    /**
     * Create a new timestamp (with time zone) column on the table.
     *
     * @param  string  $column
     * @param  int|null  $precision
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function timestampTz($column, $precision = 0) {
        return $this->addColumn('timestamp', $column);
    }

    /**
     * Create a new time column (with time zone) on the table.
     *
     * @param  string  $column
     * @param  int|null  $precision
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function timeTz($column, $precision = 0) {
        return $this->addColumn('time', $column);
    }

    /**
     * Create a new timeuuid column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function timeuuid($column) {
        return $this->addColumn('timeuuid', $column);
    }

    /**
     * Create a new auto-incrementing tiny integer (1-byte) column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function tinyIncrements($column) {
        throw new RuntimeException('This database driver does not support auto-increment columns.');
    }

    /**
     * Create a new tinyint column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function tinyint($column) {
        return $this->addColumn('tinyint', $column);
    }

    /**
     * Create a new tiny integer (1-byte) column on the table.
     *
     * @param  string  $column
     * @param  bool  $autoIncrement
     * @param  bool  $unsigned
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function tinyInteger($column, $autoIncrement = false, $unsigned = false) {
        return $this->addColumn('tinyint', $column);
    }

    /**
     * Create a new tiny text column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function tinyText($column) {
        return $this->addColumn('varchar', $column);
    }

    /**
     * Create a new tuple column on the table.
     *
     * @param string $column
     * @param string $tuple1type
     * @param string $tuple2type
     * @param string $tuple3type
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function tuple($column, $tuple1type, $tuple2type, $tuple3type) {
        return $this->addColumn('tuple', $column, compact('tuple1type', 'tuple2type', 'tuple3type'));
    }

    /**
     * Specify a unique index for the table.
     *
     * @param  string|array<mixed>  $columns
     * @param  string|null  $name
     * @param  string|null  $algorithm
     * @return \Illuminate\Database\Schema\IndexDefinition
     */
    public function unique($columns, $name = null, $algorithm = null) {
        throw new RuntimeException('This database driver does not support unique indexes.');
    }

    /**
     * Create a new varchar column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function varchar($column) {
        return $this->addColumn('varchar', $column);
    }

    /**
     * Create a new varint column on the table.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent<string,mixed>
     */
    public function varint($column) {
        return $this->addColumn('varint', $column);
    }
    /**
     * Create a new year column on the table.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function year($column) {
        return $this->addColumn('date', $column);
    }

    /**
     * Add the index commands fluently specified on columns.
     *
     * @param  \Illuminate\Database\Connection  $connection
     * @param  \Illuminate\Database\Schema\Grammars\Grammar  $grammar
     * @return void
     */
    protected function addFluentIndexes(Connection $connection, Grammar $grammar) {
        foreach ($this->columns as $column) {
            foreach (['partition', 'clustering', 'primary', 'unique', 'index', 'fulltext', 'fullText', 'spatialIndex'] as $index) {

                if (!isset($column->{$index})) {
                    continue;
                }

                $columnName = isset($column->name) ? $column->name : null;

                // If the index has been specified on the given column, but is simply equal
                // to "true" (boolean), no name has been specified for this index so the
                // index method can be called without a name and it will generate one.
                if ($column->{$index} === true) {
                    $this->{$index}($columnName);
                    $column->{$index} = null;

                    continue 2;
                }

                // If the index has been specified on the given column, but it equals false
                // and the column is supposed to be changed, we will call the drop index
                // method with an array of column to drop it by its conventional name.
                elseif ($column->{$index} === false && !empty($column->change)) {
                    $this->{'drop' . ucfirst($index)}([$columnName]);
                    $column->{$index} = null;

                    continue 2;
                }

                // If the index has been specified on the given column, and it has a string
                // value, we'll go ahead and call the index method and pass the name for
                // the index since the developer specified the explicit name for this.
                elseif (isset($column->{$index})) {
                    $this->{$index}($columnName, $column->{$index});
                    $column->{$index} = null;

                    continue 2;
                }
            }
        }
    }
}
