phpkafka
========

PHP extension for **Apache Kafka 0.8**. It's built on top of kafka C driver ([librdkafka](https://github.com/edenhill/librdkafka/)).
It makes persistent connection to kafka broker with non-blocking calls, so it should be very fast.

IMPORTANT: Library is in heavy development and some features are not implemented yet.

Requirements:
-------------
Download and install [librdkafka](https://github.com/edenhill/librdkafka/). Run `sudo ldconfig` to update shared libraries.

Installing PHP extension:
----------
```bash
phpize
./configure --enable-kafka --with-php-config=/path/to/php-config
make
sudo make install
sudo sh -c 'echo "extension=kafka.so" >> /etc/php5/conf.d/kafka.ini'
#For CLI mode:
sudo sh -c 'echo "extension=kafka.so" >> /etc/php5/cli/conf.d/20-kafka.ini'
```

Examples:
--------
```php
// Produce a message
$kafka = new Kafka("localhost:9092");
$kafka->set_topic("topic_name");
$kafka->produce("message content");
```

```php
// Consume a message
$kafka = new Kafka("localhost:9092");
$kafka->set_topic("topic_name");
$msg = $kafka->consume(10,10);
print_r($msg);
```
