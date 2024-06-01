<?php

declare(strict_types=1);

namespace LaraCassandra\Events;

use Cassandra\Response\Result;
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
     */
    public Result $statement;

    /**
     * Create a new event instance.
     *
     * @param  \LaraCassandra\Connection $connection
     * @return void
     */
    public function __construct(Connection $connection, Result $statement) {
        $this->statement = $statement;
        $this->connection = $connection;
    }
}
