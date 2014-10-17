# coding: utf-8

require 'redis_counters/cluster'
require 'redis_counters/partition'

module RedisCounters
  module ClusterizeAndPartitionize
    # Public: Возвращает массив партиций (подпартиций) кластера в виде хешей.
    #
    # Если партиция не указана, возвращает все партиции кластера.
    #
    # params - Hash - хеш параметров, определяющий кластер и партицию.
    #
    # Партиция может быть не задана, тогда будут возвращены все партиции кластера.
    # Может быть задана не листовая партиция, тогда будут все её листовые подпартции.
    #
    # Returns Array Of Hash.
    #
    def partitions(params = {})
      partitions_keys(params).map do |part|
        # parse and exclude counter_name and cluster
        part = part.split(key_delimiter, -1).from(1).from(cluster_keys.size)
        # construct hash
        Hash[partition_keys.zip(part)].with_indifferent_access
      end
    end

    # Public: Возвращает данные счетчика для указанной кластера из указанных партиций.
    #
    # params - Hash - хеш параметров, определяющий кластер и партицию.
    #
    # Партиция может быть не задана, тогда будут возвращены все партиции кластера.
    # Может быть задана не листовая партиция, тогда будут все её листовые подпартции.
    #
    # Если передан блок, то вызывает блок для каждой партиции.
    # Если блок, не передн, то аккумулирует данные,
    # из всех запрошенных партиций, и затем возвращает их.
    #
    # Returns Array Of Hash.
    #
    def data(params = {})
      total_rows = 0
      cluster = ::RedisCounters::Cluster.new(self, params).params
      parts = partitions(params).map { |partition| ::RedisCounters::Partition.new(self, partition).params }

      result = parts.flat_map do |partition|
        rows = partition_data(cluster, partition)
        total_rows += rows.size
        block_given? ? yield(rows) : rows
      end

      block_given? ? total_rows : result
    end

    # Public: Транзакционно удаляет данные указанной партиций или всех её подпартиций.
    #
    # params - Hash - хеш параметров, определяющий кластер и партицию.
    #
    # Партиция может быть не задана, тогда будут возвращены все партиции кластера.
    # Может быть задана не листовая партиция, тогда будут все её листовые подпартции.
    #
    # Если передан блок, то вызывает блок, после удаления всех данных, в транзакции.
    #
    # Returns Nothing.
    #
    def delete_partitions!(params = {})
      parts = partitions(params)

      transaction do
        parts.each { |partition| delete_partition_direct!(params.merge(partition)) }
        yield if block_given?
      end
    end

    # Public: Транзакционно удаляет все данные счетчика в кластере.
    # Если кластеризация не используется, то удаляет все данные.
    #
    # cluster - Hash - хеш параметров, определяющих кластер.
    #                  Опционально, если кластеризация не используется.
    #
    # Если передан блок, то вызывает блок, после удаления всех данных, в транзакции.
    #
    # Returns Nothing.
    #
    def delete_all!(cluster = {})
      parts = partitions(cluster)

      transaction do
        delete_all_direct!(cluster, redis, parts)
        yield if block_given?
      end
    end

    # Public: Нетранзакционно удаляет данные конкретной конечной партиции.
    #
    # params        - Hash - хеш параметров, определяющий кластер и листовую партицию.
    #
    # write_session - Redis - соединение с Redis, в рамках которого
    #                 будет производится удаление (опционально).
    #                 По умолчанию - основное соединение счетчика.
    #
    # Должна быть задана конкретная листовая партиция.
    #
    # Returns Nothing.
    #
    def delete_partition_direct!(params = {}, write_session = redis)
      cluster = ::RedisCounters::Cluster.new(self, params).params
      partition = ::RedisCounters::Partition.new(self, params).params(:only_leaf => true)
      key = key(partition, cluster)
      write_session.del(key)
    end

    # Public: Нетранзакционно удаляет все данные счетчика в кластере.
    # Если кластеризация не используется, то удаляет все данные.
    #
    # cluster       - Hash - хеш параметров, определяющих кластер.
    # write_session - Redis - соединение с Redis, в рамках которого
    #                 будет производится удаление (опционально).
    #                 По умолчанию - основное соединение счетчика.
    #
    # Returns Nothing.
    #
    def delete_all_direct!(cluster, write_session = redis, parts = partitions(cluster))
      parts.each do |partition|
        delete_partition_direct!(cluster.merge(partition), write_session)
      end
    end

    protected

    def key(partition = partition_params, cluster = cluster_params)
      raise 'Array required' if partition && !partition.is_a?(Array)
      raise 'Array required' if cluster && !cluster.is_a?(Array)

      [counter_name, cluster, partition].flatten.join(key_delimiter)
    end

    def cluster_params
      cluster_keys.map { |key| params.fetch(key) }
    end

    def partition_params
      partition_keys.map do |key|
        key.respond_to?(:call) ? key.call(params) : params.fetch(key)
      end
    end

    def cluster_keys
      @cluster_keys ||= Array.wrap(options.fetch(:cluster_keys, []))
    end

    def partition_keys
      @partition_keys ||= Array.wrap(options.fetch(:partition_keys, []))
    end

    def use_partitions?
      partition_keys.present?
    end

    def set_params(params)
      @params = params.with_indifferent_access
      check_cluster_params
    end

    def form_cluster_params(cluster_params = params)
      RedisCounters::Cluster.new(self, cluster_params).params
    end

    alias_method :check_cluster_params, :form_cluster_params

    # Protected: Возвращает массив листовых партиций в виде ключей.
    #
    # params - Hash - хеш параметров, определяющий кластер и партицию.
    #
    # Если кластер не указан и нет кластеризации в счетчике, то возвращает все партиции.
    # Партиция может быть не задана, тогда будут возвращены все партиции кластера (все партиции, если нет кластеризации).
    # Может быть задана не листовая партиция, тогда будут все её листовые подпартции.
    #
    # Returns Array of Hash.
    #
    def partitions_keys(params = {})
      cluster = ::RedisCounters::Cluster.new(self, params).params
      partition = ::RedisCounters::Partition.new(self, params).params

      strict_pattern = key(partition, cluster) if (cluster.present? && partition_keys.blank?) || partition.present?
      fuzzy_pattern = key(partition << '*', cluster)

      result = []
      result |= redis.keys(strict_pattern) if strict_pattern.present?
      result |= redis.keys(fuzzy_pattern) if fuzzy_pattern.present?
      result
    end
  end
end