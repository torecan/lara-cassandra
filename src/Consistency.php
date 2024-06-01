<?php

declare(strict_types=1);

namespace LaraCassandra;

use Cassandra\Request\Request as CassandraRequest;

enum Consistency: int {
    case ALL = CassandraRequest::CONSISTENCY_ALL;
    case ANY = CassandraRequest::CONSISTENCY_ANY;
    case EACH_QUORUM = CassandraRequest::CONSISTENCY_EACH_QUORUM;
    case LOCAL_ONE = CassandraRequest::CONSISTENCY_LOCAL_ONE;
    case LOCAL_QUORUM = CassandraRequest::CONSISTENCY_LOCAL_QUORUM;
    case LOCAL_SERIAL = CassandraRequest::CONSISTENCY_LOCAL_SERIAL;
    case ONE = CassandraRequest::CONSISTENCY_ONE;
    case QUORUM = CassandraRequest::CONSISTENCY_QUORUM;
    case SERIAL = CassandraRequest::CONSISTENCY_SERIAL;
    case THREE = CassandraRequest::CONSISTENCY_THREE;
    case TWO = CassandraRequest::CONSISTENCY_TWO;
}
