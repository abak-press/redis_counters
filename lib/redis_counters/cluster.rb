# coding: utf-8

require 'redis_counters/bucket'

module RedisCounters

  class Cluster < Bucket
    def self.default_options
      {:only_leaf => true}
    end

    protected

    def bucket_keys
      counter.send(:cluster_keys)
    end

    def required?
      true
    end
  end
end