#!/usr/bin/env php
<?php
chdir(__DIR__);

$returnStatus = null;
passthru('composer install', $returnStatus);
if ($returnStatus !== 0) {
    exit(1);
}

passthru('vendor/bin/phpcs -p --standard=psr2 src tests build.php', $returnStatus);
if ($returnStatus !== 0) {
    exit(1);
}

passthru('vendor/bin/phpunit --coverage-text=coverageText --coverage-html=coverageHtml tests', $returnStatus);
if ($returnStatus !== 0) {
    exit(1);
}

if (!strpos(file_get_contents('coverageText'), 'Lines: 100.00%')) {
    echo "Coverage NOT 100%\n";
    exit(1);
}

echo "Coverage 100%\n";
