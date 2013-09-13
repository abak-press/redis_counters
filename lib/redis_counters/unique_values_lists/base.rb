# coding: utf-8
require 'redis_counters/base_counter'

module RedisCounters
  module UniqueValuesLists

    class Base < RedisCounters::BaseCounter
      alias_method :add, :process

      protected

      def key(partition = partition_params)
        [counter_name, group_params, partition].flatten.compact.join(key_delimiter)
      end

      def group_params
        group_keys.map { |key| params.fetch(key) }
      end

      def partition_params
        partition_keys.map { |key| params.fetch(key) }
      end

      def value
        value_params = value_keys.map { |key| params.fetch(key) }
        value_params.join(value_delimiter)
      end

      def use_partitions?
        partition_keys.present?
      end

      def value_keys
        @value_keys ||= Array.wrap(options.fetch(:value_keys))
      end

      def partition_keys
        @partition_keys ||= Array.wrap(options.fetch(:partition_keys, []))
      end

      def group_keys
        @group_keys ||= Array.wrap(options.fetch(:group_keys, []))
      end
    end

  end
end