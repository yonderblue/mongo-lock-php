<?php
namespace Gaillard\Mongo;

final class LockerTest extends \PHPUnit_Framework_TestCase
{
    private $collection;
    private $dataCollection;
    private $locker;

    const TEST_DB_NAME = 'lockerTests';

    public function setUp()
    {
        parent::setUp();

        $db = (new \MongoClient())->selectDB(self::TEST_DB_NAME);
        $db->drop();

        $this->collection = $db->selectCollection('locks');
        $this->dataCollection = $db->selectCollection('data');
        $this->locker = new Locker($this->collection);
    }

    /**
     * @test
     */
    public function writeLockEmptyCollection()
    {
        $this->locker->writeLock('theId', 1000);

        $this->assertSame(1, $this->collection->count());

        $actual = $this->collection->findOne();

        $actualWriteStaleTs = $actual['writeStaleTs']->sec;
        unset($actual['writeStaleTs']);

        $expected = ['_id' => 'theId', 'readers' => [], 'writePending' => false, 'writing' => true];
        ksort($actual);
        $this->assertSame($expected, $actual);

        $this->assertLessThanOrEqual(time() + 1000, $actualWriteStaleTs);
        $this->assertGreaterThan(time() + 990, $actualWriteStaleTs);
    }

    /**
     * @test
     */
    public function writeLockClearStuckWrite()
    {
        $this->locker->writeLock('theId', 0);

        $this->locker->writeLock('theId', 1000);
    }

    /**
     * @test
     */
    public function writeLockClearStuckRead()
    {
        $this->locker->readLock('theId', 0);

        $this->locker->writeLock('theId', 1000);
    }

    /**
     * @test
     * @expectedException \Exception
     * @expectedExceptionMessage timed out waiting for lock
     */
    public function writeLockTimeout()
    {
        $locker = new Locker($this->collection, 100000, 1);

        $locker->writeLock('theId', 1000);
        $locker->writeLock('theId', 1000);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $staleDuration must be an int >= 0
     */
    public function writeLockNonIntStaleDuration()
    {
        (new Locker($this->collection))->writeLock('theId', true);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $staleDuration must be an int >= 0
     */
    public function writeLockNegativeStaleDuration()
    {
        (new Locker($this->collection))->writeLock('theId', -1);
    }

    /**
     * @test
     */
    public function readLockEmptyCollection()
    {
        $readerId = $this->locker->readLock('theId', 1000);

        $this->assertSame(1, $this->collection->count());

        $actual = $this->collection->findOne();

        $actualReaders = $actual['readers'];
        unset($actual['readers']);

        $expected = ['_id' => 'theId', 'writePending' => false, 'writeStaleTs' => null, 'writing' => false];

        ksort($actual);
        $this->assertSame($expected, $actual);

        $this->assertCount(1, $actualReaders);
        $this->assertCount(2, $actualReaders[0]);

        $this->assertInstanceOf('\MongoId', $actualReaders[0]['id']);

        $this->assertLessThanOrEqual(time() + 1000, $actualReaders[0]['staleTs']->sec);
        $this->assertGreaterThan(time() + 990, $actualReaders[0]['staleTs']->sec);
    }

    /**
     * @test
     */
    public function readLockClearStuck()
    {
        $this->locker->writeLock('theId', 0);

        $this->locker->readLock('theId', 1000);
    }

    /**
     * @test
     */
    public function readLockClearStuckWritePending()
    {
        $this->locker->readLock('theId', 2);

        //the attempted write lock below only sets writePending since we already have a read lock
        $locker = new Locker($this->collection, 0, 1);
        try {
            $locker->writeLock('theId', 0);
            $this->fail();
        } catch (\Exception $e) {
        }

        $this->locker->readLock('theId', 1000);
    }

    /**
     * @test
     * @expectedException \Exception
     * @expectedExceptionMessage timed out waiting for lock
     */
    public function readLockTimeout()
    {
        $locker = new Locker($this->collection, 100000, 1);

        $locker->writeLock('theId', 1000);
        $locker->readLock('theId', 1000);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $staleDuration must be an int >= 0
     */
    public function readLockNonIntStaleDuration()
    {
        (new Locker($this->collection))->readLock('theId', true);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $staleDuration must be an int >= 0
     */
    public function readLockNegativeStaleDuration()
    {
        (new Locker($this->collection))->readLock('theId', -1);
    }

    /**
     * @test
     */
    public function writeUnlock()
    {
        $this->locker->writeLock('theId', 1000);
        $this->locker->writeUnlock('theId');

        $this->assertSame(0, $this->collection->count());
    }

    /**
     * @test
     */
    public function readUnlockEmptyCollection()
    {
        $readerId = $this->locker->readLock('theId', 1000);
        $this->locker->readUnlock('theId', $readerId);

        $this->assertSame(0, $this->collection->count());
    }

    /**
     * @test
     */
    public function readUnlockExistingReader()
    {
        $existingReaderId = $this->locker->readLock('theId', 1000);

        $readerId = $this->locker->readLock('theId', 1000);
        $this->locker->readUnlock('theId', $readerId);

        $this->assertSame(1, $this->collection->count());

        $actual = $this->collection->findOne();

        $actualReaders = $actual['readers'];
        unset($actual['readers']);

        $expected = ['_id' => 'theId', 'writePending' => false, 'writeStaleTs' => null, 'writing' => false];

        ksort($actual);
        $this->assertSame($expected, $actual);

        $this->assertCount(1, $actualReaders);
        $this->assertCount(2, $actualReaders[0]);

        $this->assertInstanceOf('\MongoId', $actualReaders[0]['id']);

        $this->assertLessThanOrEqual(time() + 1000, $actualReaders[0]['staleTs']->sec);
        $this->assertGreaterThan(time() + 990, $actualReaders[0]['staleTs']->sec);
    }

    /**
     * @test
     */
    public function readUnlockExistingReaderStale()
    {
        $this->locker->readLock('theId', 0);

        $readerId = $this->locker->readLock('theId', 1000);
        $this->locker->readUnlock('theId', $readerId);

        $this->assertSame(0, $this->collection->count());
    }

    /**
     * @test
     */
    public function twoWriters()
    {
        $writer = function ($keyOne, $keyTwo, $keyThree) {
            $db = (new \MongoClient())->selectDB(self::TEST_DB_NAME);
            $dataCollection = $db->selectCollection('data');
            $locker = new Locker($db->selectCollection('locks'), 0);

            for ($i = 0; $i < 500; ++$i) {
                $locker->writeLock('theId', 1000);

                $dataCollection->update(['_id' => 1], ['_id' => 1, 'key' => $keyOne], ['upsert' => true]);
                $dataCollection->update(['_id' => 2], ['_id' => 2, 'key' => $keyTwo], ['upsert' => true]);
                $dataCollection->update(['_id' => 3], ['_id' => 3, 'key' => $keyThree], ['upsert' => true]);

                $docs = iterator_to_array($dataCollection->find([], ['_id' => 0])->sort(['_id' => 1]), false);
                if ($docs !== [['key' => $keyOne], ['key' => $keyTwo], ['key' => $keyThree]]) {
                    $dataCollection->update(['_id' => 'fail'], ['_id' => 'fail'], ['upsert' => true]);
                }

                $locker->writeUnlock('theId');
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

        $this->assertNull($this->dataCollection->findOne(['_id' => 'fail']));
    }

    /**
     * @test
     */
    public function oneWriterOneReader()
    {
        $reader = function () {
            $db = (new \MongoClient())->selectDB(self::TEST_DB_NAME);
            $dataCollection = $db->selectCollection('data');
            $locker = new Locker($db->selectCollection('locks'), 0);

            while (true) {
                $readerId = $locker->readLock('theId', 1000);

                $docs = iterator_to_array($dataCollection->find([], ['_id' => 0])->sort(['_id' => 1]), false);
                if ($docs !== [] && $docs !== [['key' => 1], ['key' => 2], ['key' => 3]]) {
                    $dataCollection->update(['_id' => 'fail'], ['_id' => 'fail'], ['upsert' => true]);
                }

                $locker->readUnlock('theId', $readerId);
            }
        };

        $writer = function () {
            $db = (new \MongoClient())->selectDB(self::TEST_DB_NAME);
            $dataCollection = $db->selectCollection('data');
            $locker = new Locker($db->selectCollection('locks'), 0);

            for ($i = 0; $i < 1000; ++$i) {
                $locker->writeLock('theId', 1000);

                $dataCollection->update(['_id' => 1], ['_id' => 1, 'key' => 1], ['upsert' => true]);
                $dataCollection->update(['_id' => 2], ['_id' => 2, 'key' => 2], ['upsert' => true]);
                $dataCollection->update(['_id' => 3], ['_id' => 3, 'key' => 3], ['upsert' => true]);

                $locker->writeUnlock('theId');
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

        $this->assertNull($this->dataCollection->findOne(['_id' => 'fail']));
    }

    /**
     * @test
     */
    public function twoWritersTwoReaders()
    {
        $reader = function () {
            $db = (new \MongoClient())->selectDB(self::TEST_DB_NAME);
            $dataCollection = $db->selectCollection('data');
            $locker = new Locker($db->selectCollection('locks'), 0);

            while (true) {
                $readerId = $locker->readLock('theId', 1000);

                $docs = iterator_to_array($dataCollection->find([], ['_id' => 0])->sort(['_id' => 1]), false);
                if ($docs !== [] &&
                    $docs !== [['key' => 1], ['key' => 2], ['key' => 3]] &&
                    $docs !== [['key' => 4], ['key' => 5], ['key' => 6]]) {
                    $dataCollection->update(['_id' => 'fail'], ['_id' => 'fail'], ['upsert' => true]);
                }

                $locker->readUnlock('theId', $readerId);
            }
        };

        $writer = function ($keyOne, $keyTwo, $keyThree) {
            $db = (new \MongoClient())->selectDB(self::TEST_DB_NAME);
            $dataCollection = $db->selectCollection('data');
            $locker = new Locker($db->selectCollection('locks'), 0);

            for ($i = 0; $i < 200; ++$i) {
                $locker->writeLock('theId', 1000);

                $dataCollection->update(['_id' => 1], ['_id' => 1, 'key' => $keyOne], ['upsert' => true]);
                $dataCollection->update(['_id' => 2], ['_id' => 2, 'key' => $keyTwo], ['upsert' => true]);
                $dataCollection->update(['_id' => 3], ['_id' => 3, 'key' => $keyThree], ['upsert' => true]);

                $locker->writeUnlock('theId');
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

        $this->assertNull($this->dataCollection->findOne(['_id' => 'fail']));
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $pollDuration must be an int >= 0
     */
    public function nonIntPollDuration()
    {
        new Locker($this->collection, true);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $pollDuration must be an int >= 0
     */
    public function negativePollDuration()
    {
        new Locker($this->collection, -1);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $timeoutDuration must be an int >= 0
     */
    public function nonIntTimeoutDuration()
    {
        new Locker($this->collection, 0, true);
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage $timeoutDuration must be an int >= 0
     */
    public function negativeTimeoutDuration()
    {
        new Locker($this->collection, 0, -1);
    }
}
