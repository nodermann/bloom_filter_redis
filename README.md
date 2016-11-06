# bloom_filter_redis
A simple implementation of a bloom filter for the redis

WARN: Delete the previous filter, if you change the capacity or the error_rate, otherwise the lookup function will be incorrect

The error is consistently different why? https://en.wikipedia.org/wiki/Law_of_large_numbers
