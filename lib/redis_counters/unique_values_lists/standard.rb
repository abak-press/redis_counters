# coding: utf-8
require 'redis_counters/unique_values_lists/base'

module RedisCounters
  module UniqueValuesLists

    # Список уникальных значений, на основе механизма оптимистических блокировок.
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

      # Public: Нетранзакционно удаляет данные конкретной конечной партиции.
      #
      # params        - Hash - хеш параметров, определяющий кластер и партицию.
      # write_session - Redis - соединение с Redis, в рамках которого
      #                 будет производится удаление (опционально).
      #                 По умолчанию - основное соединение счетчика.
      #
      # Если передан блок, то вызывает блок, после удаления всех данных, в транзакции.
      #
      # Returns Nothing.
      #
      def delete_partition_direct!(params = {}, write_session = redis)
        super(params, write_session)

        # удаляем партицию из списка
        return unless use_partitions?

        cluster = ::RedisCounters::Cluster.new(self, params).params
        partition = ::RedisCounters::Partition.new(self, params).params(:only_leaf => true)

        partition = partition.flatten.join(key_delimiter)
        write_session.lrem(partitions_list_key(cluster), 0, partition)
      end

      protected

      def key(partition = partition_params, cluster = cluster_params)
        return super if use_partitions?
        [counter_name, cluster, partition].flatten.compact.join(key_delimiter)
      end

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
        all_partitions.each do |partition|
          redis.watch(key(partition))
        end
      end

      def value_already_exists?
        all_partitions.reverse.any? do |partition|
          redis.sismember(key(partition), value)
        end
      end

      def add_value
        redis.sadd(key, value)
      end

      def all_partitions(cluster = cluster_params)
        return @partitions unless @partitions.nil?
        return (@partitions = [nil]) unless use_partitions?

        @partitions = redis.lrange(partitions_list_key(cluster), 0, -1)
        @partitions = @partitions.map do |partition|
          partition.split(key_delimiter, -1)
        end
          .delete_if(&:empty?)
      end

      def add_partition
        return unless use_partitions?
        return unless new_partition?
        redis.rpush(partitions_list_key, current_partition)
      end

      def partitions_list_key(cluster = cluster_params)
        [counter_name, cluster, PARTITIONS_LIST_POSTFIX].flatten.join(key_delimiter)
      end

      def current_partition
        partition_params.flatten.join(key_delimiter)
      end

      def new_partition?
        !all_partitions.include?(current_partition.split(key_delimiter))
      end


      # Protected: Возвращает массив листовых партиций в виде ключей.
      #
      # Если кластер не указан и нет кластеризации в счетчике, то возвращает все партиции.
      # Если партиция не указана, возвращает все партиции кластера (все партиции, если нет кластеризации).
      #
      # params  - Hash - хеш параметров, определяющий кластер и партицию.
      # parts   - Array of Hash - список партиций.
      #
      # Returns Array of Hash.
      #
      def partitions_keys(params = {})
        reset_partitions_cache

        cluster = ::RedisCounters::Cluster.new(self, params).params
        partition = ::RedisCounters::Partition.new(self, params).params

        partitions_keys = all_partitions(cluster).map { |part| key(part, cluster) }

        fuzzy_pattern = key(partition, cluster)
        partitions_keys.select { |part| part.start_with?(fuzzy_pattern) }
      end
    end
  end
end