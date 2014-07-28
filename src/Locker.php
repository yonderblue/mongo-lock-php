<?php
namespace Gaillard\Mongo;

final class Locker
{
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
     * Get a read lock.
     *
     * @param \MongoCollection $collection the lock collection
     * @param mixed $id an id for the lock that used with the other *Lock()/*Unlock() methods.
     *     Any type suitable for a mongo _id
     * @param \MongoDate $staleTimestamp time the read is considered stale and can be cleared.
     *     (to possibly write lock if no more readers)
     * @param int $pollDuration duration in microseconds to wait inbetween lock attempts
     * @param int $timeoutTimestamp a unix timestamp to stop waiting and throw an exception
     *
     * @throws \Exception
     *
     * @return \MongoId a reader id to be given to readUnlock()
     */
    public static function readLock(
        \MongoCollection $collection,
        $id,
        \MongoDate $staleTimestamp,
        $pollDuration = 100000,
        $timeoutTimestamp = PHP_INT_MAX
    ) {
        if (!is_int($pollDuration) || $pollDuration < 0) {
            throw new \InvalidArgumentException('$pollDuration must be an int >= 0');
        }

        if (!is_int($timeoutTimestamp)) {
            throw new \InvalidArgumentException('$timeoutTimestamp must be an int');
        }

        while (time() < $timeoutTimestamp) {
            $readerId = new \MongoId();
            $query = ['_id' => $id, 'writing' => false, 'writePending' => false];
            $update = [
                '$push' => ['readers' => ['id' => $readerId, 'staleTs' => $staleTimestamp]],
                '$set' => ['writeStaleTs' => null],
            ];
            try {
                if ($collection->update($query, $update, ['upsert' => true])['n'] === 1) {
                    return $readerId;
                }
            } catch (\MongoException $e) {
                if ($e->getCode() !== 11000) {
                    throw $e;//@codeCoverageIgnore
                }
            }

            if (self::clearStuckWrite($collection, $id)) {
                continue;
            }

            usleep($pollDuration);
        }

        throw new \Exception('timed out waiting for lock');
    }

    /**
     * Release a read lock.
     *
     * @param \MongoCollection $collection the lock collection
     * @param mixed $id an id for the lock that used with the other *Lock()/*Unlock() methods.
     *     Any type suitable for a mongo _id
     * @param \MongoId $readerId reader id returned from readLock()
     */
    public static function readUnlock(\MongoCollection $collection, $id, \MongoId $readerId)
    {
        $collection->update(['_id' => $id], ['$pull' => ['readers' => ['id' => $readerId]]]);
    }

    /**
     * Get a write lock.
     *
     * @param \MongoCollection $collection the lock collection
     * @param mixed $id an id for the lock that used with the other *Lock()/*Unlock() methods.
     *     Any type suitable for a mongo _id
     * @param \MongoDate $staleTimestamp time the write is considered stale and can be cleared.
     *     (to possibly read/write lock)
     * @param int $pollDuration duration in microseconds to wait inbetween lock attempts
     * @param int $timeoutTimestamp a unix timestamp to stop waiting and throw an exception
     *
     * @throws \Exception
     */
    public static function writeLock(
        \MongoCollection $collection,
        $id,
        \MongoDate $staleTimestamp,
        $pollDuration = 100000,
        $timeoutTimestamp = PHP_INT_MAX
    ) {
        if (!is_int($pollDuration) || $pollDuration < 0) {
            throw new \InvalidArgumentException('$pollDuration must be an int >= 0');
        }

        if (!is_int($timeoutTimestamp)) {
            throw new \InvalidArgumentException('$timeoutTimestamp must be an int');
        }

        while (time() < $timeoutTimestamp) {
            $query = ['_id' => $id, 'writing' => false, 'readers' => ['$size' => 0]];
            $update = ['writing' => true, 'writePending' => false, 'writeStaleTs' => $staleTimestamp, 'readers' => []];
            try {
                if ($collection->update($query, $update, ['upsert' => true])['n'] === 1) {
                    return;
                }
            } catch (\MongoException $e) {
                if ($e->getCode() !== 11000) {
                    throw $e;//@codeCoverageIgnore
                }
            }

            if (self::clearStuckWrite($collection, $id) || self::clearStuckRead($collection, $id)) {
                continue;
            }

            $collection->update(['_id' => $id], ['$set' => ['writePending' => true]]);

            usleep($pollDuration);
        }

        throw new \Exception('timed out waiting for lock');
    }

    /**
     * Release a write lock.
     *
     * @param \MongoCollection $collection the lock collection
     * @param mixed $id an id for the lock that used with the other *Lock()/*Unlock() methods.
     *     Any type suitable for a mongo _id
     */
    public static function writeUnlock(\MongoCollection $collection, $id)
    {
        $collection->remove(['_id' => $id]);
    }

    private static function clearStuckWrite(\MongoCollection $collection, $id)
    {
        return $collection->remove(
            ['_id' => $id, 'writing' => true, 'writeStaleTs' => ['$lte' => new \MongoDate()]]
        )['n'] === 1;
    }

    private static function clearStuckRead(\MongoCollection $collection, $id)
    {
        $now = new \MongoDate();
        $query = ['_id' => $id, 'writing' => false, 'readers.staleTs' => ['$lte' => $now]];
        $update = ['$pull' => ['readers' => ['staleTs' => ['$lte' => $now]]]];
        return $collection->update($query, $update)['n'] === 1;
    }
}
