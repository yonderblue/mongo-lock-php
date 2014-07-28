<?php
namespace Gaillard\Mongo;

final class LockerTest extends \PHPUnit_Framework_TestCase
{
    const TEST_DB_NAME = 'mongoUtilTests';

    public function setUp()
    {
        parent::setUp();

        (new \MongoClient())->selectDB(self::TEST_DB_NAME)->drop();
    }

    /**
     * @test
     */
    public function writeLockEmptyCollection()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');

        $staleTimestamp = new \MongoDate(time() + 1000);

        Locker::writeLock($collection, 'theId', $staleTimestamp);

        $this->assertSame(1, $collection->count());
        $expected = [
            '_id' => 'theId',
            'writing' => true,
            'writeStaleTs' => $staleTimestamp,
            'writePending' => false,
            'readers' => [],
        ];
        $this->assertEquals($expected, $collection->findOne());
    }

    /**
     * @test
     */
    public function writeLockClearStuckWrite()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');

        Locker::writeLock($collection, 'theId', new \MongoDate());

        Locker::writeLock($collection, 'theId', new \MongoDate(time() + 1000));
    }

    /**
     * @test
     */
    public function writeLockClearStuckRead()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');

        Locker::readLock($collection, 'theId', new \MongoDate());

        Locker::writeLock($collection, 'theId', new \MongoDate(time() + 1000));
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $pollDuration must be an int >= 0
     */
    public function writeLockNonIntPollDuration()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');
        Locker::writeLock($collection, 'theId', new \MongoDate(), true);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $pollDuration must be an int >= 0
     */
    public function writeLockNegativePollDuration()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');
        Locker::writeLock($collection, 'theId', new \MongoDate(), -1);
    }

    /**
     * @test
     * @expectedException \Exception
     * @expectedExceptionMessage timed out waiting for lock
     */
    public function writeLockTimeout()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');
        Locker::writeLock($collection, 'theId', new \MongoDate(time() + 1000));
        Locker::writeLock($collection, 'theId', new \MongoDate(time() + 1000), 100000, time() + 1);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $timeoutTimestamp must be an int
     */
    public function writeLockTimeoutNotInt()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');
        Locker::writeLock($collection, 'theId', new \MongoDate(), 0, true);
    }

    /**
     * @test
     */
    public function readLockEmptyCollection()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');

        $staleTimestamp = new \MongoDate(time() + 1000);

        $readerId = Locker::readLock($collection, 'theId', $staleTimestamp);

        $this->assertSame(1, $collection->count());
        $expected = [
            '_id' => 'theId',
            'writing' => false,
            'writePending' => false,
            'readers' => [['id' => $readerId, 'staleTs' => $staleTimestamp]],
            'writeStaleTs' => null,
        ];
        $this->assertEquals($expected, $collection->findOne());
    }

    /**
     * @test
     */
    public function readLockClearStuck()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');

        Locker::writeLock($collection, 'theId', new \MongoDate());

        Locker::readLock($collection, 'theId', new \MongoDate(time() + 1000));
    }

    /**
     * @test
     * @expectedException \Exception
     * @expectedExceptionMessage timed out waiting for lock
     */
    public function readLockTimeout()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');
        Locker::writeLock($collection, 'theId', new \MongoDate(time() + 1000));
        Locker::readLock($collection, 'theId', new \MongoDate(time() + 1000), 100000, time() + 1);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $pollDuration must be an int >= 0
     */
    public function readLockNonIntPollDuration()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');
        Locker::readLock($collection, 'theId', new \MongoDate(), true);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $pollDuration must be an int >= 0
     */
    public function readLockNegativePollDuration()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');
        Locker::readLock($collection, 'theId', new \MongoDate(), -1);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $timeoutTimestamp must be an int
     */
    public function readLockTimeoutNotInt()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');
        Locker::readLock($collection, 'theId', new \MongoDate(), 0, true);
    }

    /**
     * @test
     */
    public function writeUnlock()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');

        Locker::writeLock($collection, 'theId', new \MongoDate(time() + 1000));
        Locker::writeUnlock($collection, 'theId');

        $this->assertSame(0, $collection->count());
    }

    /**
     * @test
     */
    public function readUnlockEmptyCollection()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');

        $readerId = Locker::readLock($collection, 'theId', new \MongoDate(time() + 1000));
        Locker::readUnlock($collection, 'theId', $readerId);

        $this->assertSame(1, $collection->count());
        $expected = [
            '_id' => 'theId',
            'writePending' => false,
            'writing' => false, 'readers' => [],
            'writeStaleTs' => null,
        ];
        $this->assertSame($expected, $collection->findOne());
    }

    /**
     * @test
     */
    public function readUnlockExistingReader()
    {
        $collection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('locks');

        $existingStaleTimestamp = new \MongoDate(time() + 1000);
        $existingReaderId = Locker::readLock($collection, 'theId', $existingStaleTimestamp);

        $readerId = Locker::readLock($collection, 'theId', new \MongoDate(time() + 1000));
        Locker::readUnlock($collection, 'theId', $readerId);

        $this->assertSame(1, $collection->count());
        $expected = [
            '_id' => 'theId',
            'writing' => false,
            'writePending' => false,
            'readers' => [['id' => $existingReaderId, 'staleTs' => $existingStaleTimestamp]],
            'writeStaleTs' => null,
        ];
        $this->assertEquals($expected, $collection->findOne());
    }

    /**
     * @test
     */
    public function twoWriters()
    {
        $writer = function ($keyOne, $keyTwo, $keyThree) {
            $db = (new \MongoClient())->selectDB(self::TEST_DB_NAME);
            $dataCollection = $db->selectCollection('data');
            $locksCollection = $db->selectCollection('locks');

            for ($i = 0; $i < 500; ++$i) {
                Locker::writeLock($locksCollection, 'theId', new \MongoDate(time() + 1000), 0);

                $dataCollection->update(['_id' => 1], ['key' => $keyOne], ['upsert' => true]);
                $dataCollection->update(['_id' => 2], ['key' => $keyTwo], ['upsert' => true]);
                $dataCollection->update(['_id' => 3], ['key' => $keyThree], ['upsert' => true]);

                $docs = iterator_to_array($dataCollection->find([], ['_id' => 0])->sort(['_id' => 1]));
                if ($docs !== [['key' => $keyOne], ['key' => $keyTwo], ['key' => $keyThree]]) {
                    $dataCollection->update(['_id' => 'fail'], [], ['upsert' => true]);
                }

                Locker::writeUnlock($locksCollection, 'theId');
            }
        };


        $writerOnePid = pcntl_fork();
        if ($writerOnePid === 0) {
            $writer(1, 2, 3);
            exit;
        }

        //parent as writer two
        $writer(4, 5, 6);

        posix_kill($writerOnePid, SIGTERM);

        $dataCollection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('data');
        $this->assertNull($dataCollection->findOne(['_id' => 'fail']));
    }

    /**
     * @test
     */
    public function oneWriterOneReader()
    {
        $reader = function () {
            $db = (new \MongoClient())->selectDB(self::TEST_DB_NAME);
            $dataCollection = $db->selectCollection('data');
            $locksCollection = $db->selectCollection('locks');

            while (true) {
                $readerId = Locker::readLock($locksCollection, 'theId', new \MongoDate(time() + 1000), 0);

                $docs = iterator_to_array($dataCollection->find([], ['_id' => 0])->sort(['_id' => 1]));
                if ($docs !== [['key' => 1], ['key' => 2], ['key' => 3]]) {
                    $dataCollection->update(['_id' => 'fail'], [], ['upsert' => true]);
                }

                Locker::readUnlock($locksCollection, 'theId', $readerId);
            }
        };

        $writer = function () {
            $db = (new \MongoClient())->selectDB(self::TEST_DB_NAME);
            $dataCollection = $db->selectCollection('data');
            $locksCollection = $db->selectCollection('locks');

            for ($i = 0; $i < 1000; ++$i) {
                Locker::writeLock($locksCollection, 'theId', new \MongoDate(time() + 1000), 0);

                $dataCollection->update(['_id' => 1], ['key' => 1], ['upsert' => true]);
                $dataCollection->update(['_id' => 2], ['key' => 2], ['upsert' => true]);
                $dataCollection->update(['_id' => 3], ['key' => 3], ['upsert' => true]);

                Locker::writeUnlock($locksCollection, 'theId');
            }
        };

        $readerPid = pcntl_fork();
        if ($readerPid === 0) {
            $reader();
            exit;
        }

        //parent as writer
        $writer();

        posix_kill($readerPid, SIGTERM);

        $dataCollection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('data');
        $this->assertNull($dataCollection->findOne(['_id' => 'fail']));
    }

    /**
     * @test
     */
    public function twoWritersTwoReaders()
    {
        $reader = function () {
            $db = (new \MongoClient())->selectDB(self::TEST_DB_NAME);
            $dataCollection = $db->selectCollection('data');
            $locksCollection = $db->selectCollection('locks');

            while (true) {
                $readerId = Locker::readLock($locksCollection, 'theId', new \MongoDate(time() + 1000), 0);

                $docs = iterator_to_array($dataCollection->find([], ['_id' => 0])->sort(['_id' => 1]));
                if ($docs !== [['key' => 1], ['key' => 2], ['key' => 3]] &&
                    $docs !== [['key' => 4], ['key' => 5], ['key' => 6]]) {
                    $dataCollection->update(['_id' => 'fail'], [], ['upsert' => true]);
                }

                Locker::readUnlock($locksCollection, 'theId', $readerId);
            }
        };

        $writer = function ($keyOne, $keyTwo, $keyThree) {
            $db = (new \MongoClient())->selectDB(self::TEST_DB_NAME);
            $dataCollection = $db->selectCollection('data');
            $locksCollection = $db->selectCollection('locks');

            for ($i = 0; $i < 200; ++$i) {
                Locker::writeLock($locksCollection, 'theId', new \MongoDate(time() + 1000), 0);

                $dataCollection->update(['_id' => 1], ['key' => $keyOne], ['upsert' => true]);
                $dataCollection->update(['_id' => 2], ['key' => $keyTwo], ['upsert' => true]);
                $dataCollection->update(['_id' => 3], ['key' => $keyThree], ['upsert' => true]);

                Locker::writeUnlock($locksCollection, 'theId');
            }
        };

        $readerOnePid = pcntl_fork();
        if ($readerOnePid === 0) {
            $reader();
            exit;
        }

        $readerTwoPid = pcntl_fork();
        if ($readerTwoPid === 0) {
            $reader();
            exit;
        }

        $writerOnePid = pcntl_fork();
        if ($writerOnePid === 0) {
            $writer(1, 2, 3);
            exit;
        }

        //parent as writer two
        $writer(4, 5, 6);

        posix_kill($writerOnePid, SIGTERM);
        posix_kill($readerOnePid, SIGTERM);
        posix_kill($readerTwoPid, SIGTERM);

        $dataCollection = (new \MongoClient())->selectDB(self::TEST_DB_NAME)->selectCollection('data');
        $this->assertNull($dataCollection->findOne(['_id' => 'fail']));
    }
}
