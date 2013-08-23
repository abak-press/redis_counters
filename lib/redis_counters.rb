# encoding: utf-8
require 'redis_counters/version'
require 'redis_counters/base_counter'
require 'redis_counters/hash_counter'
require 'redis_counters/unique_hash_counter'
require 'redis_counters/unique_values_list'

require 'active_support/core_ext'

module RedisCounters

  def create_counter(redis, opts)
    BaseCounter.create(redis, opts)
  end

  module_function :create_counter
end