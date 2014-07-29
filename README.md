# mongo-lock-php

[![Build Status](https://travis-ci.org/gaillard/mongo-lock-php.png)](https://travis-ci.org/gaillard/mongo-lock-php)

Distributed multi-reader lock using MongoDB

## Requirements

Requires PHP 5.4.0 (or later).

## Installation
To add the library as a local, per-project dependency use [Composer](http://getcomposer.org)!
```json
{
    "require": {
        "gaillard/mongo-lock": "~1.0"
    }
}
```

## Example

```php
$writer = function($value) {
    $db = (new \MongoClient())->selectDB('locksExample');
    $data = $db->selectCollection('data');
    $locks = $db->selectCollection('locks');

    while (true) {
        Locker::writeLock($locks, 'theId', new \MongoDate(time() + 1000), 0);

        $data->update(['_id' => 1], ['_id' => 1, 'key' => $value], ['upsert' => true]);
        $data->update(['_id' => 2], ['_id' => 2, 'key' => $value], ['upsert' => true]);
        $data->update(['_id' => 3], ['_id' => 3, 'key' => $value], ['upsert' => true]);
        $data->update(['_id' => 4], ['_id' => 4, 'key' => $value], ['upsert' => true]);

        Locker::writeUnlock($locks, 'theId');
    }
};

$reader = function() {
    $db = (new \MongoClient())->selectDB('locksExample');
    $data = $db->selectCollection('data');
    $locks = $db->selectCollection('locks');

    while (true) {
        $readerId = Locker::readLock($locks, 'theId', new \MongoDate(time() + 1000), 100000);

        foreach ($data->find()->sort(['_id' => 1]) as $doc) {
            echo "{$doc['key']} ";
        }

        echo "\n";

        Locker::readUnlock($locks, 'theId', $readerId);

        usleep(100000);
    }
};

$writerOnePid = pcntl_fork();
if ($writerOnePid === 0) {
    $writer('pie');
    exit;
}

$writerTwoPid = pcntl_fork();
if ($writerTwoPid === 0) {
    $writer('cake');
    exit;
}

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

sleep(4);

posix_kill($writerOnePid, SIGTERM);
posix_kill($writerTwoPid, SIGTERM);
posix_kill($readerOnePid, SIGTERM);
posix_kill($readerTwoPid, SIGTERM);
```

prints something similiar too

```sh
pie pie pie pie
pie pie pie pie pie
pie pie pie
pie pie pie pie pie pie pie pie

cake cake cake cake cake cake cake
cake
pie pie pie pie
cake cake cake cake
pie pie pie pie
cake cake cake cake
```
You'll notice that all the pies and cakes are always seperated by a newline. That is because there are no readers with a lock during writing.
That also indicates both writers not writing at the same time. Sometimes when running the example there are more or less than four printed
before a newline. That is the case when the two readers have a lock at the same time.

## Contributing
If you would like to contribute, please use the build process for any changes
and after the build passes, send a pull request on github!
```sh
./build.php
```
