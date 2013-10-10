# coding: utf-8
require 'redis_counters/unique_values_lists/base'

module RedisCounters
  module UniqueValuesLists

    # Список уникальных значений, на основе не блокирующего алгоритма.
    #
    # Особенности:
    #   * 2-х кратный расхзод памяти в случае использования партиций;
    #   * Не ведет список партиций;
    #   * Не транзакционен.
    #   * Методы delete_partitions! и delete_partition_direct!, удаляют только дублирующие партиции,
    #     но не удаляют данные из основной партиции.
    #     Для удаления основной партиции необходимо вызвать delete_main_partition!,
    #     либо воспользоваться методом delete_all!

    class Fast < UniqueValuesLists::Base

      # Public: Нетранзакционно удаляет все данные счетчика.
      #
      # group         - Hash - группа.
      # write_session - Redis - соединение с Redis, в рамках которого
      #                 будет производится удаление (опционально).
      #                 По умолчанию - основное соединение счетчика.
      #
      # Returns Nothing.
      #
      def delete_all_direct!(group, write_session = redis, parts = partitions(group))
        super(group, write_session, parts)
        delete_main_partition!(group, write_session)
      end

      # Public: Удаляет основную партицию.
      #
      # group         - Hash - группа.
      # write_session - Redis - соединение с Redis, в рамках которого
      #                 будет производится удаление (опционально).
      #                 По умолчанию - основное соединение счетчика.
      #
      # Returns Nothing.
      #
      def delete_main_partition!(group, write_session = redis)
        group = prepared_group(group)
        key = key([], group)
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
        group = prepared_group(group)
        prepared_parts(parts).inject(Array.new) do |result, partition|
          strict_pattern = key(partition, group) if (group.present? && partition_keys.blank?) || partition.present?
          fuzzy_pattern = key(partition << '*', group)
          result |= redis.keys(strict_pattern) if strict_pattern.present?
          result |= redis.keys(fuzzy_pattern) if fuzzy_pattern.present?
          result
        end
      end
    end

  end
end