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
      # partition     - Hash - партиция.
      # write_session - Redis - соединение с Redis, в рамках которого
      #                 будет производится удаление (опционально).
      #                 По умолчанию - основное соединение счетчика.
      #
      # Если передан блок, то вызывает блок, после удаления всех данных, в транзакции.
      #
      # Returns Nothing.
      #
      def delete_partition_direct!(group, partition = {}, write_session = redis)
        super(group, partition, write_session)

        # удаляем партицию из списка
        return unless use_partitions?
        group = prepared_group(group)
        partition = prepared_parts(partition, :only_leaf => true)
        partition = partition.flatten.join(key_delimiter)
        write_session.lrem(partitions_list_key(group), 0, partition)
      end

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

      def all_partitions(group = group_params)
        return @partitions unless @partitions.nil?
        return (@partitions = [nil]) unless use_partitions?

        @partitions = redis.lrange(partitions_list_key(group), 0, -1)
        @partitions = @partitions.map do |partition|
          partition.split(key_delimiter)
        end
          .delete_if(&:empty?)
      end

      def add_partition
        return unless use_partitions?
        return unless new_partition?
        redis.rpush(partitions_list_key, current_partition)
      end

      def partitions_list_key(group = group_params)
        [counter_name, group, PARTITIONS_LIST_POSTFIX].flatten.join(key_delimiter)
      end

      def current_partition
        partition_params.flatten.join(key_delimiter)
      end

      def new_partition?
        !all_partitions.include?(current_partition.split(key_delimiter))
      end


      # Protected: Возвращает массив листовых партиций в виде ключей.
      #
      # Если группа не указана и нет группировки в счетчике, то возвращает все партиции.
      # Если партиция не указана, возвращает все партиции группы (все партиции, если нет группировки).
      #
      # group - Hash - группа.
      # parts - Array of Hash - список партиций.
      #
      # Returns Array of Hash.
      #
      def partitions_raw(group = {}, parts = {})
        reset_partitions_cache
        group = prepared_group(group)
        partitions_keys = all_partitions(group).map { |partition| key(partition, group) }

        prepared_parts(parts).flat_map do |partition|
          strict_pattern = key(partition, group) if (group.present? && partition_keys.blank?) || partition.present?
          fuzzy_pattern = key(partition << '', group)
          partitions_keys.select { |part| part.eql?(strict_pattern) } |
            partitions_keys.select { |part| part.start_with?(fuzzy_pattern) }
        end.uniq
      end
    end

  end
end