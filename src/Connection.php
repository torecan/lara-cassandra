<?php

declare(strict_types=1);

namespace LaraCassandra;

use Closure;

use Illuminate\Database\Connection as BaseConnection;

use Cassandra\Connection as CassandraConnection;
use Cassandra\Exception as CassandraException;
use Cassandra\Response\Result as CassandraResult;
use Illuminate\Support\Facades\Log;
use LaraCassandra\Events\StatementPrepared;

class Connection extends BaseConnection {
    public const DEFAULT_CONNECT_TIMEOUT = 5.0;
    public const DEFAULT_CONSISTENCY = Consistency::LOCAL_ONE;
    public const DEFAULT_PAGE_SIZE = 500;
    public const DEFAULT_REQUEST_TIMEOUT = 12.0;
    public const DEFAULT_TIMEOUT = 30;

    /**
     * The active CDO connection.
     */
    protected CassandraConnection|Closure|null $cdo;

    protected Consistency $consistency = self::DEFAULT_CONSISTENCY;

    protected bool $logWarnings = true;

    /**
     * The active CDO connection used for reads.
     */
    protected CassandraConnection|Closure|null $readCdo;

    protected ?Closure $warningHandler = null;

    /**
     * Create a new database connection instance.
     *
     * @param array<string,mixed> $config
     *
     * @return void
     */
    public function __construct(array $config) {

        /** 
         * @var array{
         *   name: string,
         *   host: string,
         *   port: string|int,
         *   username: string,
         *   password: string,
         *   keyspace?: string,
         *   prefix?: string,
         *   timeout?: int,
         *   connect_timeout?: float,
         *   request_timeout?: float,
         *   page_size?: int
         * } $config
         */
        $this->cdo = $this->createNativeConnection($config);
        $this->readCdo = null;

        $this->setReconnector(function ($connection) use ($config) {
            $connection->disconnect();
            $connection->cdo = $connection->createNativeConnection($config);
        });

        $keyspace = $config['keyspace'] ?? '';
        $tablePrefix = $config['prefix'] ?? '';

        // First we will setup the default properties. We keep track of the DB
        // name we are connected to since it is needed when some reflective
        // type commands are run such as checking whether a table exists.
        $this->database = $keyspace;

        $this->tablePrefix = $tablePrefix;

        $this->config = $config;

        // We need to initialize a query grammar and the query post processors
        // which are both very important parts of the database abstractions
        // so we initialize these to their default values while starting.
        $this->useDefaultQueryGrammar();

        $this->useDefaultPostProcessor();
    }

    /**
     * Run an SQL statement and get the number of rows affected.
     *
     * @param  string  $query
     * @param  array<mixed>  $bindings
     * @return int
     */
    public function affectingStatement($query, $bindings = []) {
        $result = $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return 0;
            }

            $cdo = $this->getCdo();

            // For update or delete statements, we want to get the number of rows affected
            // by the statement and return that back to the developer. We'll first need
            // to execute the statement and then we'll use PDO to fetch the affected.
            $prepareResult = $cdo->prepare($query);
            $this->logResultWarnings($prepareResult, $query);

            $preparedBindings = $this->prepareBindings($bindings);

            $result = $cdo->executeSync(
                $prepareResult,
                $preparedBindings,
                $this->getConsistency()->value
            );
            $this->logResultWarnings($result, $query);

            // we can't get the affected rows count from Cassandra,
            // so we assume that one row was affected
            $count = 1;

            $this->recordsHaveBeenModified(true);

            return $count;
        });

        if (!is_int($result)) {
            throw new CassandraException('Result is not integer');
        }

        return $result;
    }

    /**
     * Run a select statement against the database and returns a generator.
     *
     * @param  string  $query
     * @param  array<mixed>  $bindings
     * @param  bool  $useReadPdo
     * @return \Generator
     */
    public function cursor($query, $bindings = [], $useReadPdo = true) {

        $useReadCdo = $useReadPdo;

        $results = $this->run($query, $bindings, function ($query, $bindings) use ($useReadCdo) {
            if ($this->pretending()) {
                return [];
            }

            $cdo = $this->getCdoForSelect($useReadCdo);

            // First we will create a statement for the query. Then, we will set the fetch
            // mode and prepare the bindings for the query. Once that's done we will be
            // ready to execute the query against the database and return the cursor.
            $prepareResult = $cdo->prepare($query);
            $this->logResultWarnings($prepareResult, $query);

            $this->event(new StatementPrepared($this, $prepareResult));

            $preparedBindings = $this->prepareBindings($bindings);

            // Next, we'll execute the query against the database and return the statement
            // so we can return the cursor. The cursor will use a PHP generator to give
            // back one row at a time without using a bunch of memory to render them.
            $results = [];
            $pagingState = null;
            do {
                $options = [
                    'page_size' => $this->getPageSize(),
                ];

                if ($pagingState) {
                    $options['paging_state'] = $pagingState;
                }

                $result = $cdo->executeSync(
                    $prepareResult,
                    $preparedBindings,
                    $this->getConsistency()->value,
                    $options,
                );
                $this->logResultWarnings($result, $query);

                $pagingState = $result->getMetadata()['paging_state'] ?? null;

                $results[] = $result->getIterator();

            } while ($pagingState);

            return $result->getIterator();
        });

        if (!is_iterable($results)) {
            throw new CassandraException('Results are not iterable');
        }

        foreach ($results as $result) {
            if (!is_iterable($result)) {
                throw new CassandraException('Result is not iterable');
            }

            foreach ($result as $record) {
                yield $record;
            }
        }
    }

    /**
     * Disconnect from the underlying CDO connection.
     *
     * @return void
     */
    public function disconnect() {

        if ($this->cdo instanceof CassandraConnection) {
            $this->cdo->disconnect();
        }

        if ($this->readCdo instanceof CassandraConnection) {
            $this->readCdo->disconnect();
        }

        $this->setCdo(null)->setReadCdo(null);
    }

    /**
     * Get the current CDO connection.
     *
     * @return \Cassandra\Connection
     */
    public function getCdo(): CassandraConnection {
        if ($this->cdo instanceof Closure) {
            return $this->cdo = call_user_func($this->cdo);
        } elseif ($this->cdo === null) {
            throw new CassandraException('CDO connection is not set');
        }

        return $this->cdo;
    }

    public function getConsistency(): Consistency {
        return $this->consistency;
    }

    /**
     * Get the name of the connected keyspace.
     *
     * @return string
     */
    public function getKeyspaceName() {
        return $this->database;
    }

    public function getPageSize(): int {
        return $this->config['page_size'] ?? self::DEFAULT_PAGE_SIZE;
    }

    /**
     * Get the current CDO connection parameter without executing any reconnect logic.
     *
     * @return \Cassandra\Connection|\Closure|null
     */
    public function getRawCdo(): CassandraConnection|Closure|null {
        return $this->cdo;
    }

    /**
     * Get the current read CDO connection parameter without executing any reconnect logic.
     *
     * @return \Cassandra\Connection|\Closure|null
     */
    public function getRawReadCdo(): CassandraConnection|Closure|null {
        return $this->readCdo;
    }

    /**
     * Get the current CDO connection used for reading.
     *
     * @return \Cassandra\Connection
     */
    public function getReadCdo(): CassandraConnection {
        if ($this->transactions > 0) {
            return $this->getCdo();
        }

        if ($this->readOnWriteConnection
            || ($this->recordsModified && $this->getConfig('sticky'))) {
            return $this->getCdo();
        }

        if ($this->readCdo instanceof Closure) {
            return $this->readCdo = call_user_func($this->readCdo);
        }

        return $this->readCdo ?: $this->getCdo();
    }

    /**
     * @inheritdoc
     */
    public function getSchemaBuilder() {

        if (is_null($this->schemaGrammar)) {
            $this->useDefaultSchemaGrammar();
        }

        return new Schema\Builder($this);
    }

    /**
     * Returns the connection grammer
     * 
     * @return Schema\Grammar
     */
    public function getSchemaGrammar() {
        return new Schema\Grammar;
    }

    /**
     * Get the server version for the connection.
     *
     * @return string
     */
    public function getServerVersion(): string {
        return (string) $this->getCdo()->getVersion();
    }

    public function ignoreWarnings(): void {
        $this->logWarnings = false;
    }

    public function logWarnings(): void {
        $this->logWarnings = true;
    }

    /**
     * @inheritdoc
     */
    public function query() {
        return new Query\Builder(
            $this, $this->getQueryGrammar(), $this->getPostProcessor()
        );
    }

    /**
     * Reconnect to the database if a CDO connection is missing.
     *
     * @return void
     */
    public function reconnectIfMissingConnection() {
        if (is_null($this->cdo)) {
            $this->reconnect();
        }
    }

    /**
     * Run a select statement against the database.
     *
     * @param  string  $query
     * @param  array<mixed>  $bindings
     * @param  bool  $useReadPdo
     * @return array<mixed>
     */
    public function select($query, $bindings = [], $useReadPdo = true) {

        $useReadCdo = $useReadPdo;

        $result = $this->run($query, $bindings, function ($query, $bindings) use ($useReadCdo) {
            if ($this->pretending()) {
                return [];
            }

            $cdo = $this->getCdoForSelect($useReadCdo);

            // For select statements, we'll simply execute the query and return an array
            // of the database result set. Each element in the array will be a single
            // row from the database table, and will either be an array or objects.
            $prepareResult = $cdo->prepare($query);
            $this->logResultWarnings($prepareResult, $query);

            $this->event(new StatementPrepared($this, $prepareResult));

            $preparedBindings = $this->prepareBindings($bindings);

            $result = [];
            $pagingState = null;
            do {
                $options = [
                    'page_size' => $this->getPageSize(),
                ];

                if ($pagingState) {
                    $options['paging_state'] = $pagingState;
                }

                $currentResult = $cdo->executeSync(
                    $prepareResult,
                    $preparedBindings,
                    $this->getConsistency()->value,
                    $options,
                );
                $this->logResultWarnings($currentResult, $query);

                $pagingState = $currentResult->getMetadata()['paging_state'] ?? null;

                $result = array_merge($result, $currentResult->fetchAll());

            } while ($pagingState);

            return $result;
        });

        if (!is_array($result)) {
            throw new CassandraException('Result is not an array');
        }

        return $result;
    }

    /**
      * Run a select statement against the database and returns all of the result sets.
      *
      * @param  string  $query
      * @param  array<mixed>  $bindings
      * @param  bool  $useReadPdo
      * @return mixed
      */
    public function selectResultSets($query, $bindings = [], $useReadPdo = true) {

        return $this->select($query, $bindings, $useReadPdo);
    }

    /**
     * Set the CDO connection.
     *
     * @param  \Cassandra\Connection|\Closure|null  $cdo
     * @return $this
     */
    public function setCdo(CassandraConnection|Closure|null $cdo): self {
        $this->transactions = 0;

        $this->cdo = $cdo;

        return $this;
    }

    public function setConsistency(Consistency $consistency): self {
        $this->consistency = $consistency;

        return $this;
    }

    public function setDefaultConsistency(): void {
        $this->consistency = $this->config['consistency'] ?? self::DEFAULT_CONSISTENCY;
    }

    /**
     * Set the name of the connected keyspace.
     *
     * @param  string  $keyspace
     * @return $this
     */
    public function setKeyspaceName($keyspace) {
        $this->database = $keyspace;

        return $this;
    }

    /**
     * Set the CDO connection used for reading.
     *
     * @param  \Cassandra\Connection|\Closure|null  $cdo
     * @return $this
     */
    public function setReadCdo(CassandraConnection|Closure|null $cdo): self {
        $this->readCdo = $cdo;

        return $this;
    }

    public function setWarningHandler(Closure $handler): void {
        $this->warningHandler = $handler;
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param  string  $query
     * @param  array<mixed>  $bindings
     * @return bool
     */
    public function statement($query, $bindings = []) {
        $result = $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return true;
            }

            $cdo = $this->getCdo();

            $prepareResult = $cdo->prepare($query);
            $this->logResultWarnings($prepareResult, $query);

            $preparedBindings = $this->prepareBindings($bindings);

            $this->recordsHaveBeenModified();

            $result = $cdo->executeSync(
                $prepareResult,
                $preparedBindings,
                $this->getConsistency()->value
            );
            $this->logResultWarnings($result, $query);

            return true;
        });

        if (!is_bool($result)) {
            throw new CassandraException('Result is not boolean');
        }

        return $result;
    }

    /**
     * Run a raw, unprepared query against the PDO connection.
     *
     * @param  string  $query
     * @return bool
     */
    public function unprepared($query) {
        $result = $this->run($query, [], function ($query) {
            if ($this->pretending()) {
                return true;
            }

            $cdo = $this->getCdo();

            $result = $cdo->querySync(
                $query,
                [],
                $this->getConsistency()->value,
            );
            $this->logResultWarnings($result, $query);

            if ($result->getKind() === CassandraResult::ROWS) {
                $count = $result->getRowCount();
            } else {
                $count = 0;
            }

            $change = $count > 0;

            $this->recordsHaveBeenModified($change);

            return $change;
        });

        if (!is_bool($result)) {
            throw new CassandraException('Result is not boolean');
        }

        return $result;
    }

    /**
     * Create a new native Cassandra connection.
     *
     * @param array{
     *   host: string,
     *   port: string|int,
     *   username: string,
     *   password: string,
     *   keyspace?: string,
     *   timeout?: int,
     *   connect_timeout?: float,
     *   request_timeout?: float,
     *   page_size?: int
     * } $config
     * 
     * @return CassandraConnection
     */
    protected function createNativeConnection(array $config): CassandraConnection {
        $nodes = $this->getNodes($config);
        $nativeConnection = new CassandraConnection($nodes, $config['keyspace'] ?? '');
        $nativeConnection->connect();

        return $nativeConnection;
    }

    /**
     * Escape a binary value for safe SQL embedding.
     *
     * @param  string  $value
     * @return string
     */
    protected function escapeBinary($value) {
        $hex = bin2hex($value);

        return '0x' . $hex;
    }

    /**
     * Escape a boolean value for safe SQL embedding.
     *
     * @param  bool  $value
     * @return string
     */
    protected function escapeBool($value) {

        return $value ? 'true' : 'false';
    }

    /**
     * Escape a string value for safe SQL embedding.
     *
     * @param  string  $value
     * @return string
     */
    protected function escapeString($value) {

        return "'" . str_replace("'", "''", $value) . "'";
    }

    /**
     * Get the CDO connection to use for a select query.
     *
     * @param  bool  $useReadCdo
     * @return \Cassandra\Connection
     */
    protected function getCdoForSelect($useReadCdo = true): CassandraConnection {
        return $useReadCdo ? $this->getReadCdo() : $this->getCdo();
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultPostProcessor() {
        return new Query\Processor();
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultQueryGrammar() {
        ($grammar = new Query\Grammar)->setConnection($this);

        return $grammar;
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultSchemaGrammar() {
        ($grammar = new Schema\Grammar)->setConnection($this);

        $grammar->setTablePrefix($this->tablePrefix);

        return $grammar;
    }

    /**
     * Get nodes config
     *
     * @param array{
     *   host: string,
     *   port: string|int,
     *   username: string,
     *   password: string,
     *   keyspace?: string,
     *   timeout?: int,
     *   connect_timeout?: float,
     *   request_timeout?: float,
     *   page_size?: int
     * } $config
     * 
     * @return array<array{
     *   host: string,
     *   port: int,
     *   username: string,
     *   password: string,
     *   timeout: int,
     *   connect_timeout: float,
     *   request_timeout: float,
     *   page_size: int
     * }>
     */
    protected function getNodes(array $config): array {
        $nodes = [];

        $hosts = explode(',', $config['host']);
        $config['port'] = $config['port'] ?? [];

        if (count($hosts) < 1) {
            throw new CassandraException('DB hostname is not found, please check your DB hostname');
        }

        if ($config['port']) {
            if (is_string($config['port'])) {
                $ports = explode(',', $config['port']);
            } else {
                $ports = [array_fill(0, count($hosts), $config['port'])];
            }
        } else {
            $ports = array_fill(0, count($hosts), 9042);
        }

        foreach ($hosts as $index => $host) {
            $node = [
                'host' => $host,
                'port' => (int) $ports[$index],
                'username' => $config['username'],
                'password' => $config['password'],
                'timeout' => $config['timeout'] ?? self::DEFAULT_TIMEOUT,
                'connect_timeout' => $config['connect_timeout'] ?? self::DEFAULT_CONNECT_TIMEOUT,
                'request_timeout' => $config['request_timeout'] ?? self::DEFAULT_REQUEST_TIMEOUT,
                'page_size' => $config['page_size'] ?? self::DEFAULT_PAGE_SIZE,
            ];

            $nodes[] = $node;
        }

        return $nodes;
    }

    protected function logResultWarnings(CassandraResult $result, string $query): void {

        if (!$this->logWarnings) {
            return;
        }

        $warnings = $result->getWarnings();
        if ($warnings) {
            foreach ($warnings as $warning) {

                $warningMessage = 'Warning - ' . $warning . ': ' . $query;

                if ($this->warningHandler) {
                    call_user_func($this->warningHandler, $warningMessage);
                } else {
                    Log::warning($warningMessage);
                }
            }
        }
    }
}
