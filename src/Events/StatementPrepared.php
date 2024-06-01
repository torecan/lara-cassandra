<?php

declare(strict_types=1);

namespace LaraCassandra\Events;

use LaraCassandra\Connection;

class StatementPrepared {
    /**
     * The database connection instance.
     *
     * @var \LaraCassandra\Connection
     */
    public Connection $connection;

    /**
     * The CDO statement.
     *
     * @var array<mixed>
     */
    public array $statement;

    /**
     * Create a new event instance.
     *
     * @param  \LaraCassandra\Connection $connection
     * @param  array<mixed> $statement
     * @return void
     */
    public function __construct(Connection $connection, array $statement) {
        $this->statement = $statement;
        $this->connection = $connection;
    }
}
