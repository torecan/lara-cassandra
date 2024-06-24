<?php

declare(strict_types=1);

namespace LaraCassandra;

use Illuminate\Database\Migrations\MigrationRepositoryInterface;
use Illuminate\Database\ConnectionResolverInterface as Resolver;
use Illuminate\Support\Facades\DB;
use LaraCassandra\Query\Builder as QueryBuilder;
use LaraCassandra\Schema\Builder as SchemaBuilder;
use RuntimeException;

class CassandraMigrationRepository implements MigrationRepositoryInterface {
    /**
     * The name of the database connection to use.
     *
     * @var string
     */
    protected $connection;
    /**
     * The database connection resolver instance.
     *
     * @var \Illuminate\Database\ConnectionResolverInterface
     */
    protected $resolver;

    /**
     * The name of the migration table.
     *
     * @var string
     */
    protected $table;

    /**
     * Create a new database migration repository instance.
     *
     * @param  \Illuminate\Database\ConnectionResolverInterface  $resolver
     * @param  string  $table
     * @return void
     */
    public function __construct(Resolver $resolver, $table) {
        $this->table = $table;
        $this->resolver = $resolver;
    }

    /**
     * Create the migration repository data store.
     *
     * @return void
     */
    public function createRepository() {
        $schema = $this->getConnection()->getSchemaBuilder();
        if (!$schema instanceof SchemaBuilder) {
            throw new RuntimeException('Schema builder must be an instance of LaraCassandra\Schema\Builder');
        }

        $schema->setConsistency(Consistency::ALL);

        $schema->create($this->table, function ($table) {
            // The migrations table is responsible for keeping track of which of the
            // migrations have actually run for the application. We'll create the
            // table to hold the migration file's path as well as the batch ID.
            $table->uuid('id')->primary();
            $table->text('migration')->index();
            $table->int('batch')->index();
        });
    }

    /**
     * Remove a migration from the log.
     *
     * @param  object  $migration
     * @return void
     */
    public function delete($migration) {

        if (!isset($migration->migration)) {
            throw new RuntimeException('Migration is missing required migration attribute');
        }

        $ids = $this->table()
            ->where('migration', $migration->migration)
            ->get()
            ->pluck('id')
            ->all();

        $this->table()->whereIn('id', $ids)->delete();
    }

    /**
     * Delete the migration repository data store.
     *
     * @return void
     */
    public function deleteRepository() {

        $schema = $this->getConnection()->getSchemaBuilder();
        if (!$schema instanceof SchemaBuilder) {
            throw new RuntimeException('Schema builder must be an instance of LaraCassandra\Schema\Builder');
        }

        $schema->setConsistency(Consistency::ALL);

        $schema->drop($this->table);
    }

    /**
     * Resolve the database connection instance.
     *
     * @return Connection
     */
    public function getConnection() {
        $connection = $this->resolver->connection($this->connection);

        if (!$connection instanceof Connection) {
            throw new RuntimeException('Connection must be an instance of LaraCassandra\Connection');
        }

        return $connection;
    }

    /**
     * Get the connection resolver instance.
     *
     * @return \Illuminate\Database\ConnectionResolverInterface
     */
    public function getConnectionResolver() {
        return $this->resolver;
    }

    /**
     * Get the last migration batch.
     *
     * @return array<mixed>
     */
    public function getLast() {

        $migrations = $this->table()
            ->where('batch', $this->getLastBatchNumber())
            ->get()->all();

        usort($migrations, function ($a, $b) {

            if (!is_array($a) || !is_array($b)) {
                throw new RuntimeException('Migration must be an array');
            }

            return strcmp($b['migration'], $a['migration']);
        });

        return $migrations;
    }

    /**
     * Get the last migration batch number.
     *
     * @return int
     */
    public function getLastBatchNumber() {

        $builder = $this->table();
        if (!$builder instanceof QueryBuilder) {
            throw new RuntimeException('Query builder must be an instance of LaraCassandra\Query\Builder');
        }

        $lastBatchNumber = $builder->ignoreWarnings()->max('batch') ?? 0;

        if (!is_numeric($lastBatchNumber)) {
            throw new RuntimeException('Batch number must be numeric');
        }

        return (int) $lastBatchNumber;
    }

    /**
     * Get the completed migrations with their batch numbers.
     *
     * @return array<mixed>
     */
    public function getMigrationBatches() {

        $migrations = $this->table()->get()->all();

        usort($migrations, function ($a, $b) {

            if (!is_array($a) || !is_array($b)) {
                throw new RuntimeException('Migration must be an array');
            }

            if ($a['batch'] === $b['batch']) {
                return strcmp($a['migration'], $b['migration']);
            }

            return $a['batch'] - $b['batch'];
        });

        return collect($migrations)->pluck('batch', 'migration')->toArray();
    }

    /**
     * Get the list of migrations.
     *
     * @param  int  $steps
     * @return array<mixed>
     */
    public function getMigrations($steps) {

        $migrations = $this->table()
            ->get()->all();

        usort($migrations, function ($a, $b) {

            if (!is_array($a) || !is_array($b)) {
                throw new RuntimeException('Migration must be an array');
            }

            if ($a['batch'] === $b['batch']) {
                return strcmp($b['migration'], $a['migration']);
            }

            return $b['batch'] - $a['batch'];
        });

        return array_splice($migrations, 0, $steps);
    }

    /**
     * Get the list of the migrations by batch number.
     *
     * @param  int  $batch
     * @return array<mixed>
     */
    public function getMigrationsByBatch($batch) {

        $migrations= $this->table()
            ->where('batch', $batch)
            ->get()
            ->all();

        usort($migrations, function ($a, $b) {

            if (!is_array($a) || !is_array($b)) {
                throw new RuntimeException('Migration must be an array');
            }

            return strcmp($b['migration'], $a['migration']);
        });

        return $migrations;
    }

    /**
     * Get the next migration batch number.
     *
     * @return int
     */
    public function getNextBatchNumber() {
        return $this->getLastBatchNumber() + 1;
    }

    /**
     * Get the completed migrations.
     *
     * @return array<mixed>
     */
    public function getRan() {

        $migrations = $this->table()->get()->all();

        usort($migrations, function ($a, $b) {

            if (!is_array($a) || !is_array($b)) {
                throw new RuntimeException('Migration must be an array');
            }

            if ($a['batch'] === $b['batch']) {
                return strcmp($a['migration'], $b['migration']);
            }

            return $a['batch'] - $b['batch'];
        });

        return collect($migrations)->pluck('migration')->toArray();
    }

    /**
     * Log that a migration was run.
     *
     * @param  string  $file
     * @param  int  $batch
     * @return void
     */
    public function log($file, $batch) {

        $record = [
            'id' => DB::raw('uuid()'),
            'migration' => $file,
            'batch' => $batch,
        ];

        $this->table()->insert($record);
    }

    /**
     * Determine if the migration repository exists.
     *
     * @return bool
     */
    public function repositoryExists() {
        $schema = $this->getConnection()->getSchemaBuilder();
        if (!$schema instanceof SchemaBuilder) {
            throw new RuntimeException('Schema builder must be an instance of LaraCassandra\Schema\Builder');
        }

        $schema->setConsistency(Consistency::ALL);

        return $schema->hasTable($this->table);
    }

    /**
     * Set the information source to gather data.
     *
     * @param  string  $name
     * @return void
     */
    public function setSource($name) {
        $this->connection = $name;
    }

    /**
     * Get a query builder for the migration table.
     *
     * @return \Illuminate\Database\Query\Builder
     */
    protected function table() {
        $builder = $this->getConnection()
            ->table($this->table)
            ->useWritePdo()
        ;

        if (!$builder instanceof QueryBuilder) {
            throw new RuntimeException('Query builder must be an instance of LaraCassandra\Query\Builder');
        }

        $builder->setConsistency(Consistency::ALL);

        return $builder;
    }
}
