# coding: utf-8
require 'redis_counters/base_counter'

module RedisCounters
  module UniqueValuesLists

    # Базовый класс списка уникальных значений,
    # с возможностью группировки и партиционирования.

    class Base < RedisCounters::BaseCounter
      alias_method :add, :process

      # Public: Возвращает данные счетчика для указанной группы из указанных партиций.
      #
      # group - Hash - группа (опционально, если не используются группы).
      # parts - Array of Hash - список партиций (опционально).
      #         По умолчанию, выбираются все данные группы.
      #
      # Если передан блок, то вызывает блок для каждой партиции.
      # Если блок, не передн, то аккумулирует данные,
      # из всех запрошенных партиций, и затем возвращает их.
      #
      # Returns Array Of Hash.
      #
      def data(group = {}, parts = {})
        parts = partitions(group, parts)
        group = prepared_group(group)
        prepared_parts(parts).flat_map do |partition|
          rows = partition_data(group, partition)
          block_given? ? yield(rows) : rows
        end
      end

      # Public: Возвращает массив партиций (подпартиций) группы в виде хешей.
      #
      # Если партиция не указана, возвращает все партиции группы.
      #
      # group - Hash - группа (опционально, если не используются группы).
      # parts - Array of Hash - список партиций (опционально).
      #         По умолчанию, выбираются все данные группы.
      #
      # Returns Array Of Hash.
      #
      def partitions(group = {}, parts = {})
        partitions_raw(group, parts).map do |part|
          # parse and exclude counter_name and group
          part = part.split(key_delimiter, -1).from(1).from(group_keys.size)
          # construct hash
          Hash[partition_keys.zip(part)].with_indifferent_access
        end
      end

      # Public: Транзакционно удаляет все данные счетчика.
      #
      # group - Hash - группа (опционально, если не используются группы).
      #
      # Если передан блок, то вызывает блок, после удаления всех данных, в транзакции.
      #
      # Returns Nothing.
      #
      def delete_all!(group)
        parts = partitions(group)
        transaction do
          delete_all_direct!(group, redis, parts)
          yield if block_given?
        end
      end

      # Public: Транзакционно удаляет данные всех указанных партиций.
      #
      # parts - Array of Hash - список партиций.
      #
      # Если передан блок, то вызывает блок, после удаления всех данных, в транзакции.
      #
      # Returns Nothing.
      #
      def delete_partitions!(group, parts)
        if parts.blank?
          raise ArgumentError, 'You must specify a partitions'
        end

        parts = Array.wrap(parts).flat_map { |part| partitions(group, part) }

        transaction do
          parts.each { |partition| delete_partition_direct!(group, partition) }
          yield if block_given?
        end
      end

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
        parts.each { |partition| delete_partition_direct!(group, partition, write_session) }
      end

      # Public: Нетранзакционно удаляет данные конкретной конечной партиции.
      #
      # group         - Hash - группа.
      # partition     - Hash - партиция.
      # write_session - Redis - соединение с Redis, в рамках которого
      #                 будет производится удаление (опционально).
      #                 По умолчанию - основное соединение счетчика.
      #
      # Returns Nothing.
      #
      def delete_partition_direct!(group, partition = {}, write_session = redis)
        group = prepared_group(group)
        partition = prepared_parts(partition, :only_leaf => true)
        key = key(partition, group)
        write_session.del(key)
      end

      protected

      def key(partition = partition_params, group = group_params)
        [counter_name, group, partition].flatten.compact.join(key_delimiter)
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


      # Protected: Возвращает группу в виде массива параметров, однозначно её идентифицирующих.
      #
      # group   - Hash - группа.
      # options - Hash - хеш опций:
      #           :only_leaf - Boolean - выбирать только листовые группы (по умолачнию - true).
      #                        Если флаг установлен в true и передана не листовая группа, то
      #                        будет сгенерировано исключение KeyError.
      #
      # Метод генерирует исключение ArgumentError, если переданы параметры не верно идентифицирующие группу.
      # Например: ключи группировки счетчика {:param1, :param2, :param3}, а переданы {:param1, :param3}.
      # Метод генерирует исключение ArgumentError, 'You must specify a group',
      # если группа передана в виде пустого хеша, но группировка используется в счетчике.
      #
      # Returns Array.
      #
      def prepared_group(group, options = {})
        if group_keys.present? && group.blank?
          raise ArgumentError, 'You must specify a group'
        end

        default_options = {:only_leaf => true}
        options.reverse_merge!(default_options)

        group = group.with_indifferent_access
        group_keys.inject(Array.new) do |result, key|
          param = (options[:only_leaf] ? group.fetch(key) : group[key])
          next result unless group.has_key?(key)
          next result << param if result.size >= group_keys.index(key)

          raise ArgumentError, 'An incorrectly specified group %s' % [group]
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
      # Каждый элемент массива, представлен в виде хеша, содержащего все параметры уникального значения.
      #
      # group - Array - листовая группа - массив параметров однозначно идентифицирующий группу.
      # partition - Array - листовая партиция - массив параметров однозначно идентифицирующий партицию.
      #
      # Returns Array of WithIndifferentAccess.
      #
      def partition_data(group, partition)
        keys = value_keys
        redis.smembers(key(partition, group)).inject(Array.new) do |result, (key, value)|
          values = key.split(value_delimiter, -1) << value.to_i
          result << Hash[keys.zip(values)].with_indifferent_access
        end
      end
    end

  end
end