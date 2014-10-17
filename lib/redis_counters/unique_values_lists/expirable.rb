# coding: utf-8

require 'redis_counters/unique_values_lists/blocking'
require 'active_support/core_ext/module/aliasing'

module RedisCounters
  module UniqueValuesLists

    # Список уникальных значений, с возможностью expire отдельных элементов.
    #
    # На основе сортированного множества.
    # http://redis4you.com/code.php?id=010
    #
    # На основе механизма оптимистических блокировок.
    # смотри Optimistic locking using check-and-set:
    # http://redis.io/topics/transactions
    #
    # Особенности:
    #   * Expire - таймаут, можно установить как на уровне счетчика,
    #     так и на уровне отдельного занчения;
    #   * Очистка возможна как в автоматическогом режиме так в и ручном;
    #   * Значения сохраняет в партициях;
    #   * Ведет список партиций;
    #   * Полностью транзакционен.
    #
    # Пример:
    #
    # counter = RedisCounters::UniqueValuesLists::Expirable.new(redis,
    #   :counter_name => :sessions,
    #   :value_keys   => [:session_id],
    #   :expire       => 10.minutes
    # )
    #
    # counter << session_id: 1
    # counter << session_id: 2
    # counter << session_id: 3, expire: :never
    #
    # counter.data
    # > [{session_id: 1}, {session_id: 2}, {session_id: 3}]
    #
    # # after 10 minutes
    #
    # counter.data
    # > [{session_id: 3}]
    #
    # counter.has_value?(session_id: 1)
    # false

    class Expirable < Blocking
      DEFAULT_AUTO_CLEAN_EXPIRED = true
      DEFAULT_VALUE_TIMEOUT = :never

      NEVER_EXPIRE_TIMESTAMP = 0

      # Public: Производит принудительную очистку expired - значений.
      #
      # cluster - Hash - параметры кластера, если используется кластеризация.
      #
      # Returns nothing.
      #
      def clean_expired(cluster = {})
        set_params(cluster)
        internal_clean_expired
      end

      protected

      def add_value
        redis.zadd(key, value_expire_timestamp, value)
      end

      def reset_partitions_cache
        super
        internal_clean_expired if auto_clean_expired?
      end

      alias_method :clean, :reset_partitions_cache

      def current_timestamp
        Time.now.to_i
      end

      def value_already_exists?
        all_partitions.reverse.any? do |partition|
          redis.zrank(key(partition), value).present?
        end
      end

      def internal_clean_expired
        all_partitions.each do |partition|
          redis.zremrangebyscore(key(partition), "(#{NEVER_EXPIRE_TIMESTAMP}", current_timestamp)
        end
      end

      def value_expire_timestamp
        timeout = params[:expire] || default_value_expire

        case timeout
          when Symbol
            NEVER_EXPIRE_TIMESTAMP
          else
            current_timestamp + timeout.to_i
        end
      end

      def default_value_expire
        @default_value_expire ||= options[:expire].try(:seconds) || DEFAULT_VALUE_TIMEOUT
      end

      def auto_clean_expired?
        @auto_clean_expired ||= options.fetch(:clean_expired, DEFAULT_AUTO_CLEAN_EXPIRED)
      end

      def partitions_with_clean(params = {})
        clean_empty_partitions(params)
        partitions_without_clean(params)
      end

      alias_method_chain :partitions, :clean

      # Protected: Производит очистку expired - значений и пустых партиций.
      #
      # params - Hash - параметры кластера, если используется кластеризация.
      #
      # Returns nothing.
      #
      def clean_empty_partitions(params)
        set_params(params)
        clean

        partitions_without_clean(params).each do |partition|
          next if redis.zcard(key(partition.values)).nonzero?
          delete_partition_direct!(params.merge(partition))
        end
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
        redis.zrangebyscore(key(partition, cluster), '-inf', '+inf').inject(Array.new) do |result, (key, value)|
          values = key.split(value_delimiter, -1) << value.to_i
          result << Hash[keys.zip(values)].with_indifferent_access
        end
      end
    end
  end
end