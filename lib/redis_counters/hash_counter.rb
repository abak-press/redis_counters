# coding: utf-8
require 'redis_counters/base_counter'

module RedisCounters

  # Счетчик на основе redis-hash, с возможностью партиционирования и группировки значений.

  class HashCounter < BaseCounter
    alias_method :increment, :process

    # Public: Возвращает массив партиций (подпартиций) в виде хешей.
    #
    # Если партиция не указана, возвращает все партиции.
    #
    # parts - Array of Hash - список партиций (опционально).
    #         По умолчанию, выбираются все данные.
    #
    # Returns Array Of Hash.
    #
    def partitions(parts = {})
      partitions_raw(parts).map do |part|
        # parse and exclude counter_name
        part = part.split(key_delimiter).from(1)
        # construct hash
        HashWithIndifferentAccess[partition_keys.zip(part)]
      end
    end

    # Public: Возвращает данные счетчика из указанных партиций.
    #
    # parts - Array of Hash - список партиций (опционально).
    #         По умолчанию, выбираются все данные.
    #
    # Если передан блок, то вызывает блок для каждой партиции.
    # Если блок, не передн, то аккумулирует данные,
    # из всех запрошенных партиций, и затем возвращает их.
    #
    # Returns Array Of Hash.
    #
    def data(parts = {})
      # получаем все подпартиции
      parts = partitions(parts)
      # подгатавливаем в необходимом виде
      parts = prepared_parts(parts)

      parts.flat_map do |partition|
        rows = partition_data(partition)
        block_given? ? yield(rows) : rows
      end
    end

    # Public: Транзакционно удаляет все данные счетчика.
    #
    # Если передан блок, то вызывает блок, после удаления всех данных, в транзакции.
    #
    # Returns Nothing.
    #
    def delete_all!(&block)
      delete_partitions!(partitions, &block)
    end

    # Public: Транзакционно удаляет данные всех указанных партиций.
    #
    # parts - Array of Hash - список партиций.
    #
    # Если передан блок, то вызывает блок, после удаления всех данных, в транзакции.
    #
    # Returns Nothing.
    #
    def delete_partitions!(parts)
      parts = Array.wrap(parts).flat_map { |part| partitions(part) }

      transaction do
        parts.each { |partition| delete_partition_direct!(partition) }
        yield if block_given?
      end
    end

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
    def delete_partition_direct!(partition, write_session = redis)
      partition = prepared_parts(partition, true)
      key = key(partition)
      write_session.del(key)
    end

    protected

    def process_value
      redis.hincrby(key, field, 1)
    end

    def key(partition = partition_params)
      [counter_name, partition].flatten.join(key_delimiter)
    end

    def partition_params
      partition_keys.map do |key|
        key.respond_to?(:call) ? key.call(params) : params.fetch(key)
      end
    end

    def field
      if group_keys.present?
        group_params = group_keys.map { |key| params.fetch(key) }
      else
        group_params = [field_name]
      end

      group_params.join(value_delimiter)
    end

    def field_name
      @field_name ||= options.fetch(:field_name)
    end

    def group_keys
      @group_keys ||= Array.wrap(options.fetch(:group_keys, []))
    end

    def partition_keys
      @partition_keys ||= Array.wrap(options.fetch(:partition_keys, []))
    end

    # Public: Возвращает массив партиций (подпартиций) в виде ключей.
    #
    # Если партиция не указана, возвращает все партиции.
    #
    # parts - Array of Hash - список партиций (опционально).
    #         По умолчанию, выбираются все данные.
    #
    # Returns Array of Hash.
    #
    def partitions_raw(parts = {})
      prepared_parts(parts).flat_map do |partition|
        strict_pattern = key(partition)
        fuzzy_pattern = key(partition << '*')
        redis.keys(strict_pattern) | redis.keys(fuzzy_pattern)
      end
        .uniq
    end

    def prepared_parts(parts, only_leaf = false)
      parts = Array.wrap(parts).map(&:with_indifferent_access)
      parts.map do |partition|
        partition_keys.inject(Array.new) do |result, key|
          param = (only_leaf ? partition.fetch(key) : partition[key])
          next result if param.nil?
          next result << param if result.size >= partition_keys.index(key)

          raise ArgumentError, 'An incorrectly specified partition %s' % [partition]
        end
      end
    end

    def partition_data(partition)
      keys = group_keys.dup << :value
      redis.hgetall(key(partition)).inject(Array.new) do |result, (key, value)|
        values = key.split(value_delimiter) << value.to_i
        values = values.from(1) unless group_keys.present?
        result << HashWithIndifferentAccess[keys.zip(values)]
      end
    end
  end

end