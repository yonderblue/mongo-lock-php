<?php
namespace Gaillard\Mongo;

final class Locker
{
    /**
     * The lock collection.
     *
     * @var \MongoCollection
     */
    private $collection;

    /**
     * The duration in microseconds to wait inbetween lock attempts.
     *
     * @var int
     */
    private $pollDuration;

    /**
     * How long to wait for a lock before throwing an exception.
     *
     * @var int
     */
    private $timeoutDuration;

    /**
     * Lock docs look like:
     * [
     *     '_id' => string, the id
     *     'writing' => boolean
     *     'writePending' => boolean, true if a writer has tried to lock, reset to false on write lock
     *     'writeStaleTs' => mongo date, time the write is considered stale and can be cleared, null if not writing
     *     'readers' => array, readers reading, empty when writing, each array value like: [
     *         [
     *             'id' => mongo id, generated and returned from readLock()
     *             'staleTs' => mongo date, time the read is considered stale and can be cleared
     *         ]
     *     ]
     * ]
     */

    /**
     * Initialize the locker.
     *
     * @param \MongoCollection $collection the lock collection
     * @param int $pollDuration duration in microseconds to wait inbetween lock attempts
     * @param int $timeoutDuration duration in seconds to wait for a lock before throwing an exception
     */
    public function __construct(\MongoCollection $collection, $pollDuration = 100000, $timeoutDuration = PHP_INT_MAX)
    {
        if (!is_int($pollDuration) || $pollDuration < 0) {
            throw new \InvalidArgumentException('$pollDuration must be an int >= 0');
        }

        if (!is_int($timeoutDuration) || $timeoutDuration < 0) {
            throw new \InvalidArgumentException('$timeoutDuration must be an int >= 0');
        }

        $this->collection = $collection;
        $this->pollDuration = $pollDuration;
        $this->timeoutDuration = $timeoutDuration;
    }

    /**
     * Get a read lock.
     *
     * @param mixed $id an id for the lock that used with the other *Lock()/*Unlock() methods.
     *     Any type suitable for a mongo _id
     * @param \MongoDate $staleTimestamp time the read is considered stale and can be cleared.
     *     (to possibly write lock if no more readers)
     *
     * @throws \Exception
     *
     * @return \MongoId a reader id to be given to readUnlock()
     */
    public function readLock($id, \MongoDate $staleTimestamp)
    {
        $timeoutTimestamp = (int)min(time() + $this->timeoutDuration, PHP_INT_MAX);

        while (time() < $timeoutTimestamp) {
            $readerId = new \MongoId();
            $query = ['_id' => $id, 'writing' => false, 'writePending' => false];
            $update = [
                '$push' => ['readers' => ['id' => $readerId, 'staleTs' => $staleTimestamp]],
                '$set' => ['writeStaleTs' => null],
            ];
            try {
                if ($this->collection->update($query, $update, ['upsert' => true])['n'] === 1) {
                    return $readerId;
                }
            } catch (\MongoException $e) {
                if ($e->getCode() !== 11000) {
                    throw $e;//@codeCoverageIgnore
                }
            }

            if ($this->clearStuckWrite($id)) {
                continue;
            }

            usleep($this->pollDuration);
        }

        throw new \Exception('timed out waiting for lock');
    }

    /**
     * Release a read lock.
     *
     * @param mixed $id an id for the lock that used with the other *Lock()/*Unlock() methods.
     *     Any type suitable for a mongo _id
     * @param \MongoId $readerId reader id returned from readLock()
     */
    public function readUnlock($id, \MongoId $readerId)
    {
        $this->collection->update(['_id' => $id], ['$pull' => ['readers' => ['id' => $readerId]]]);
        $this->collection->remove(['_id' => $id, 'writing' => false, 'readers' => ['$size' => 0]]);
    }

    /**
     * Get a write lock.
     *
     * @param mixed $id an id for the lock that used with the other *Lock()/*Unlock() methods.
     *     Any type suitable for a mongo _id
     * @param \MongoDate $staleTimestamp time the write is considered stale and can be cleared.
     *     (to possibly read/write lock)
     * @param int $timeoutTimestamp a unix timestamp to stop waiting and throw an exception
     *
     * @throws \Exception
     */
    public function writeLock($id, \MongoDate $staleTimestamp)
    {
        $timeoutTimestamp = (int)min(time() + $this->timeoutDuration, PHP_INT_MAX);

        while (time() < $timeoutTimestamp) {
            $query = ['_id' => $id, 'writing' => false, 'readers' => ['$size' => 0]];
            $update = [
                '_id' => $id,
                'writing' => true,
                'writePending' => false,
                'writeStaleTs' => $staleTimestamp,
                'readers' => [],
            ];
            try {
                if ($this->collection->update($query, $update, ['upsert' => true])['n'] === 1) {
                    return;
                }
            } catch (\MongoException $e) {
                if ($e->getCode() !== 11000) {
                    throw $e;//@codeCoverageIgnore
                }
            }

            if ($this->clearStuckWrite($id) || $this->clearStuckRead($id)) {
                continue;
            }

            $this->collection->update(['_id' => $id], ['$set' => ['writePending' => true]]);

            usleep($this->pollDuration);
        }

        throw new \Exception('timed out waiting for lock');
    }

    /**
     * Release a write lock.
     *
     * @param mixed $id an id for the lock that used with the other *Lock()/*Unlock() methods.
     *     Any type suitable for a mongo _id
     */
    public function writeUnlock($id)
    {
        $this->collection->remove(['_id' => $id]);
    }

    private function clearStuckWrite($id)
    {
        return $this->collection->remove(
            ['_id' => $id, 'writing' => true, 'writeStaleTs' => ['$lte' => new \MongoDate()]]
        )['n'] === 1;
    }

    private function clearStuckRead($id)
    {
        $now = new \MongoDate();
        $query = ['_id' => $id, 'writing' => false, 'readers.staleTs' => ['$lte' => $now]];
        $update = ['$pull' => ['readers' => ['staleTs' => ['$lte' => $now]]]];
        return $this->collection->update($query, $update)['n'] === 1;
    }
}
