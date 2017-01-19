import math
import mmh3

from redis import StrictRedis


class BloomFilter(object):
    def __init__(self, filter_name, capacity=0, error_rate=0.01, redis=None, delete_filter=False):
        """
        # WARN: Delete the previous filter, if you change the capacity or the error_rate,
        otherwise the lookup function will be incorrect
        :param filter_name: The name of the key in the redis
        :param capacity: The expected number of items in the filter
        :param error_rate: 0.01 is 1%
        :param delete_filter: delete the previous filter
        """
        if redis is None:
            self.redis = StrictRedis()
        else:
            self.redis = redis

        if not (0 < error_rate < 1):
            raise ValueError('Error_Rate must be between 0 and 1')
        if not capacity > 0:
            raise ValueError('Capacity must be > 0')

        num_slices = int(math.ceil(math.log(1 / error_rate, 2)))
        bits_per_slice = int(math.ceil(
            (capacity * abs(math.log(error_rate))) / (num_slices * (math.log(2) ** 2))))

        self.filter_name = filter_name
        self.capacity = capacity
        self.error_rate = error_rate
        self.number_of_hashes = num_slices
        self.array_size = bits_per_slice * num_slices

        bits_in_512_mb = 2 ** 32
        if self.array_size > bits_in_512_mb:
            current_size_in_mb = int(float(self.array_size) / 1024 / 1024 / 8)
            raise Exception('Bitmap limit is %s (512MB), current is %s (%sMB), see: http://redis.io/commands/SETBIT\n'
                            'Try to decrease the capacity or to increase the error_rate' %
                            (bits_in_512_mb, self.array_size, current_size_in_mb))
        if delete_filter:
            self.redis.delete(filter_name)
        if not self.redis.exists(filter_name):
            self.redis.setbit(filter_name, self.array_size - 1, 0)  # reserve memory into redis for the filter

    def info(self):
        print('Error rate is %s (%s%% of 100%%)' % (self.error_rate, self.error_rate * 100))
        print('Capacity is %s' % self.capacity)
        print('Size of RAM is %.2fMb (%s bits)' % (float(self.array_size) / 1024 / 1024 / 8, self.array_size))
        print('Number of hashes is %s' % self.number_of_hashes)

    def add(self, string):
        for seed in range(self.number_of_hashes):
            offset = mmh3.hash(string, seed) % self.array_size
            self.redis.setbit(self.filter_name, offset, 1)

    def lookup(self, string):
        for seed in range(self.number_of_hashes):
            offset = mmh3.hash(string, seed) % self.array_size
            if self.redis.getbit(self.filter_name, offset) == 0:
                return False
        return True  # WARN: True is Probably (false-positive) see: https://en.wikipedia.org/wiki/Bloom_filter


def test_bloom():
    import uuid

    capacity = 100
    number_of_experiments = 200

    b = BloomFilter('test_bloom', capacity)
    b.info()

    for x in range(capacity):
        b.add(str(uuid.uuid4()))

    experiments_results = []
    for h in range(number_of_experiments):
        false_positives_counter = 0
        for s in range(capacity):
            uu = uuid.uuid4()
            if b.lookup(str(uu)):
                false_positives_counter += 1
        experiments_results.append(false_positives_counter)

    amount_of_false_positives = 0
    for n in experiments_results:
        amount_of_false_positives += n
    avg_num = amount_of_false_positives / len(experiments_results)
    print('---\nAverage number of false positives:', avg_num, 'of', capacity)
    print('Error rate (false positives) for the test: %.2f%%' % (avg_num / capacity * 100))
    print('')
