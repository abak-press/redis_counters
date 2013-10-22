# coding: utf-8
require 'redis_counters/unique_values_lists/base'

module RedisCounters
  module UniqueValuesLists

    # Список уникальных значений, на основе не блокирующего алгоритма.
    #
    # Особенности:
    #   * 2-х кратный расход памяти в случае использования партиций;
    #   * Не ведет список партиций;
    #   * Не транзакционен.
    #   * Методы delete_partitions! и delete_partition_direct!, удаляют только дублирующие партиции,
    #     но не удаляют данные из основной партиции.
    #     Для удаления основной партиции необходимо вызвать delete_main_partition!

    class Fast < UniqueValuesLists::Base

      # Public: Удаляет основную партицию.
      #
      # cluster       - Hash - хеш параметров, определяющих кластер.
      # write_session - Redis - соединение с Redis, в рамках которого
      #                 будет производится удаление (опционально).
      #                 По умолчанию - основное соединение счетчика.
      #
      # Returns Nothing.
      #
      def delete_main_partition!(cluster, write_session = redis)
        cluster = prepared_cluster(cluster)
        key = key([], cluster)
        write_session.del(key)
      end

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
    end

  end
end