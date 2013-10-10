# coding: utf-8
require 'redis_counters/unique_values_lists/base'

module RedisCounters
  module UniqueValuesLists

    # Список уникального значений, на основе механизма оптимистических блокировок.
    #
    # смотри Optimistic locking using check-and-set:
    # http://redis.io/topics/transactions
    #
    # Особенности:
    #   * Значения сохраняет в партициях;
    #   * Ведет список партиций;
    #   * Полностью транзакционен.

    class Standard < Base
      PARTITIONS_LIST_POSTFIX = :partitions

      protected

      def process_value
        loop do
          reset_partitions_cache

          watch_partitions_list
          watch_all_partitions

          if value_already_exists?
            redis.unwatch
            return false
          end

          result = transaction do
            add_value
            add_partition
            yield redis if block_given?
          end

          return true if result.present?
        end
      end

      def reset_partitions_cache
        @partitions = nil
      end

      def watch_partitions_list
        return unless use_partitions?
        redis.watch(partitions_list_key)
      end

      def watch_all_partitions
        partitions.each do |partition|
          redis.watch(key(partition))
        end
      end

      def value_already_exists?
        partitions.reverse.any? do |partition|
          redis.sismember(key(partition), value)
        end
      end

      def add_value
        redis.sadd(key, value)
      end

      def partitions
        return @partitions unless @partitions.nil?
        return (@partitions = [nil]) unless use_partitions?

        @partitions = redis.smembers(partitions_list_key).map do |partition|
          partition.split(key_delimiter)
        end
          .delete_if(&:empty?)
      end

      def add_partition
        return unless use_partitions?
        return unless new_partition?
        redis.sadd(partitions_list_key, current_partition)
      end

      def partitions_list_key
        [counter_name, group_params, PARTITIONS_LIST_POSTFIX].flatten.join(key_delimiter)
      end

      def current_partition
        partition_params.flatten.join(key_delimiter)
      end

      def new_partition?
        !partitions.include?(current_partition.split(key_delimiter))
      end
    end

  end
end