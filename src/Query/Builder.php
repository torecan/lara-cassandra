<?php

declare(strict_types=1);

namespace LaraCassandra\Query;

use Closure;
use InvalidArgumentException;
use Illuminate\Database\Query\Builder as BaseBuilder;
use Illuminate\Pagination\Paginator;
use LaraCassandra\Connection;
use LaraCassandra\Consistency;
use RuntimeException;

class Builder extends BaseBuilder {
    public bool $allowFiltering = false;
    /**
     * The current query value bindings.
     *
     * @var array<string,array<mixed>>
     */
    public $bindings = [
        'select' => [],
        'from' => [],
        'where' => [],
        'order' => [],
        'updateCollection' => [],
        'insertCollection' => [],
    ];

    /**
     * @var string[] $collectionTypes
     */
    public array $collectionTypes = ['set', 'list', 'map'];

    protected ?Consistency $consistency = null;

    /**
     * Add a binding to the query.
     *
     * @throws \InvalidArgumentException
     */
    public function addCollectionBinding(mixed $value, string $type = 'updateCollection'): self {
        if (!array_key_exists($type, $this->bindings)) {
            throw new InvalidArgumentException("Invalid binding type: {$type}.");
        }
        $this->bindings[$type][] = $value;

        return $this;
    }

    /**
     * Allow filtering for this query
     *
     * @param  bool  $value
     * @return $this
     */
    public function allowFiltering(bool $value = true) {
        $this->allowFiltering = $value;

        return $this;
    }

    /**
     * Get a paginator only supporting simple next and previous links.
     *
     * This is more efficient on larger data-sets, etc.
     *
     * @param  int|null  $perPage
     * @param  array<string>|string  $columns
     * @param  string  $cursorName
     * @param  \Illuminate\Pagination\Cursor|string|null  $cursor
     * @return \Illuminate\Contracts\Pagination\CursorPaginator
     */
    public function cursorPaginate($perPage = 15, $columns = ['*'], $cursorName = 'cursor', $cursor = null) {

        throw new RuntimeException('Cursor pagination is not supported by Cassandra');
    }

    public function delete($id = null) {
        $this->applyConsistency();

        return parent::delete($id);
    }

    /**
    * @param array<mixed> $values
    */
    public function insert(array $values) {
        $this->applyConsistency();

        return parent::insert($values);
    }

    /**
     * Insert a collection type in Cassandra.
     */
    public function insertCollection(string $type, string $column, mixed $value): self {
        $insertCollection = compact('type', 'column', 'value');
        $this->addCollectionBinding($insertCollection, 'insertCollection');

        return $this;
    }

    /**
     * Paginate the given query into a simple paginator.
     *
     * @param  int|\Closure  $perPage
     * @param  array<string>|string  $columns
     * @param  string  $pageName
     * @param  int|null  $page
     * @param  \Closure|int|null  $total
     * @return \Illuminate\Contracts\Pagination\LengthAwarePaginator
     */
    public function paginate($perPage = 15, $columns = ['*'], $pageName = 'page', $page = null, $total = null) {
        $page = $page ?: Paginator::resolveCurrentPage($pageName);

        $total = value($total) ?? $this->getCountForPagination();
        if (!is_numeric($total)) {
            throw new InvalidArgumentException('Argument $total must be numeric');
        }

        $total = (int) $total;

        $perPage = $perPage instanceof Closure ? $perPage($total) : $perPage;

        $results = $total ? $this->get($columns) : collect();

        return $this->paginator($results, $total, $perPage, $page, [
            'path' => Paginator::resolveCurrentPath(),
            'pageName' => $pageName,
        ]);
    }

    public function setConsistency(Consistency $level): self {
        $this->consistency = $level;

        return $this;
    }

    /**
     * Get a paginator only supporting simple next and previous links.
     *
     * This is more efficient on larger data-sets, etc.
     *
     * @param  int  $perPage
     * @param  array<string>|string  $columns
     * @param  string  $pageName
     * @param  int|null  $page
     * @return \Illuminate\Contracts\Pagination\Paginator
     */
    public function simplePaginate($perPage = 15, $columns = ['*'], $pageName = 'page', $page = null) {
        throw new RuntimeException('Simple pagination is not supported by Cassandra.');
    }

    /**
    * @param array<mixed> $values
    */
    public function update(array $values) {
        $this->applyConsistency();

        return parent::update($values);
    }

    /**
     * Update a collection type in Cassandra (set, list, map).
     */
    public function updateCollection(string $type, string $column, ?string $operation = null, mixed $value = null): self {
        //Check if the type is anyone in SET, LIST or MAP. else throw ERROR.
        if (!in_array(strtolower($type), $this->collectionTypes)) {
            throw new InvalidArgumentException("Invalid binding type: {$type}, Should be any one of " . implode(', ', $this->collectionTypes));
        }
        // Here we will make some assumptions about the operator. If only 2 values are
        // passed to the method, we will assume that the operator is an equals sign
        // and keep going. Otherwise, we'll require the operator to be passed in.
        if (func_num_args() === 3) {
            $value = $operation;
            $operation = null;
        }

        $updateCollection = compact('type', 'column', 'value', 'operation');
        $this->addCollectionBinding($updateCollection, 'updateCollection');

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

    /**
    * @return array<mixed>
    */
    protected function runSelect() {
        $this->applyConsistency();

        return parent::runSelect();
    }
}
