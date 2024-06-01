<?php

declare(strict_types=1);

namespace LaraCassandra\Query;

use InvalidArgumentException;
use Illuminate\Database\Query\Builder as BaseBuilder;

use LaraCassandra\Connection;
use LaraCassandra\Consistency;
use RuntimeException;

class Builder extends BaseBuilder {
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

    public function setConsistency(Consistency $level): self {
        $this->consistency = $level;

        return $this;
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
