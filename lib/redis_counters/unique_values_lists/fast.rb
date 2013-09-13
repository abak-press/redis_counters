# coding: utf-8
require 'redis_counters/unique_values_lists/base'

module RedisCounters
  module UniqueValuesLists

    # Список уникального значений, на основе не блокирующего алгоритма.
    #
    # Особенности:
    #   * 2-х кратный расхзод памяти в случае использования партиций;
    #   * Не ведет список партиций;
    #   * Не транзакционен.

    class Fast < UniqueValuesLists::Base

      protected

      def process_value
        return unless add_value
        yield redis if block_given?
        true
      end

      def add_value
        return unless redis.sadd(main_partition_key, value)
        redis.sadd(current_partition_key, value) if use_partitions?
        true
      end

      def main_partition_key
        key([])
      end

      def current_partition_key
        key
      end

      def partitions
        redis.keys(key('*'))
      end
    end

  end
end