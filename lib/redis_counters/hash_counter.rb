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
        part = part.split(key_delimiter, -1).from(1)
        # construct hash
        Hash[partition_keys.zip(part)].with_indifferent_access
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
    # Returns Array Of Hash или общее кол-во строк данных, если передан блок.
    #
    def data(parts = {})
      total_rows = 0
      parts = partitions(parts)

      result = prepared_parts(parts).flat_map do |partition|
        rows = partition_data(partition)
        total_rows += rows.size
        block_given? ? yield(rows) : rows
      end

      block_given? ? total_rows : result
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
      partition = prepared_parts(partition, :only_leaf => true)
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

    # Protected: Возвращает массив листовых партиций в виде ключей.
    #
    # parts - Array of Hash - список партиций (опционально).
    #         По умолчанию, выбираются все данные.
    #
    # Returns Array of Hash.
    #
    def partitions_raw(parts = {})
      prepared_parts(parts).inject(Array.new) do |result, partition|
        strict_pattern = key(partition)
        fuzzy_pattern = key(partition << '*')
        result |= redis.keys(strict_pattern) if strict_pattern.present?
        result |= redis.keys(fuzzy_pattern) if fuzzy_pattern.present?
        result
      end
    end

    # Protected: Возвращает массив партиций, где каждая партиция,
    # представляет собой массив параметров, однозначно её идентифицирующих.
    #
    # parts   - Array of Hash - список партиций.
    # options - Hash - хеш опций:
    #           :only_leaf - Boolean - выбирать только листовые партиции (по умолачнию - false).
    #                        Если флаг установлен в true и передана не листовая партиция, то
    #                        будет сгенерировано исключение KeyError.
    #
    # Метод генерирует исключение ArgumentError, если переданы параметры не верно идентифицирующие партицию.
    # Например: ключи партиционирования счетчика {:param1, :param2, :param3}, а переданы {:param1, :param3}.
    #
    # Returns Array of Array.
    #
    def prepared_parts(parts, options = {})
      default_options = {:only_leaf => false}
      options.reverse_merge!(default_options)

      parts = Array.wrap(parts).map(&:with_indifferent_access)
      parts.map do |partition|
        partition_keys.inject(Array.new) do |result, key|
          param = (options[:only_leaf] ? partition.fetch(key) : partition[key])
          next result unless partition.has_key?(key)
          next result << param if result.size >= partition_keys.index(key)

          raise ArgumentError, 'An incorrectly specified partition %s' % [partition]
        end
      end
    end

    # Protected: Возвращает данные партиции в виде массива хешей.
    #
    # Каждый элемент массива, представлен в виде хеша, содержащего все параметры группировки и
    # значение счетчика в ключе :value.
    #
    # partition - Array - листовая партиция - массив параметров однозначно идентифицирующий партицию.
    #
    # Returns Array of WithIndifferentAccess.
    #
    def partition_data(partition)
      keys = group_keys.dup << :value
      redis.hgetall(key(partition)).inject(Array.new) do |result, (key, value)|
        values = key.split(value_delimiter, -1) << value.to_i
        values = values.from(1) unless group_keys.present?
        result << Hash[keys.zip(values)].with_indifferent_access
      end
    end
  end

end