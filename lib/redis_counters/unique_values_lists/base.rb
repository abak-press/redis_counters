# coding: utf-8
require 'redis_counters/base_counter'
require 'redis_counters/clusterize_and_partitionize'

module RedisCounters
  module UniqueValuesLists

    # Базовый класс списка уникальных значений,
    # с возможностью кластеризации и партиционирования.

    class Base < RedisCounters::BaseCounter
      include RedisCounters::ClusterizeAndPartitionize

      alias_method :add, :process
      alias_method :<<, :process

      # Public: Проверяет существует ли заданное значение.
      #
      # params - Hash - параметры кластера и значения.
      #
      # Returns Boolean.
      #
      def has_value?(params)
        raise NotImplementedError
      end

      protected

      def value
        value_params = value_keys.map { |key| params.fetch(key) }
        value_params.join(value_delimiter)
      end

      def value_keys
        @value_keys ||= Array.wrap(options.fetch(:value_keys))
      end

      # Protected: Возвращает данные партиции в виде массива хешей.
      #
      # Каждый элемент массива, представлен в виде хеша, содержащего все параметры уникального значения.
      #
      # cluster   - Array - листовой кластер - массив параметров однозначно идентифицирующий кластер.
      # partition - Array - листовая партиция - массив параметров однозначно идентифицирующий партицию.
      #
      # Returns Array of WithIndifferentAccess.
      #
      def partition_data(cluster, partition)
        keys = value_keys
        redis.smembers(key(partition, cluster)).inject(Array.new) do |result, (key, value)|
          values = key.split(value_delimiter, -1) << value.to_i
          result << Hash[keys.zip(values)].with_indifferent_access
        end
      end
    end

  end
end