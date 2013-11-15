# coding: utf-8

require 'redis_counters/bucket'

module RedisCounters

  class Partition < Bucket
    def self.default_options
      {:only_leaf => false}
    end

    def bucket_keys
      counter.send(:partition_keys)
    end
  end
end