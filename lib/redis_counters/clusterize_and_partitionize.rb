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
      cluster = prepared_cluster(params)
      parts = partitions(params).map { |partition| prepared_part(partition) }

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
      cluster = prepared_cluster(params)
      partition = prepared_part(params, :only_leaf => true)
      key = key(partition, cluster)
      write_session.del(key)
    end

    protected

    def key(partition = partition_params, cluster = cluster_params)
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

    # Protected: Возвращает кластер в виде массива параметров, однозначно его идентифицирующих.
    #
    # cluster - Hash - хеш параметров, определяющий кластер.
    # options - Hash - хеш опций:
    #           :only_leaf - Boolean - выбирать только листовые кластеры (по умолачнию - true).
    #                        Если флаг установлен в true и передана не листовой кластер, то
    #                        будет сгенерировано исключение KeyError.
    #
    # Метод генерирует исключение ArgumentError, если переданы параметры не верно идентифицирующие кластер.
    # Например: ключи кластеризации счетчика {:param1, :param2, :param3}, а переданы {:param1, :param3}.
    # Метод генерирует исключение ArgumentError, 'You must specify a cluster',
    # если кластер передан в виде пустого хеша, но кластеризация используется в счетчике.
    #
    # Returns Array.
    #
    def prepared_cluster(cluster, options = {})
      if cluster_keys.present? && cluster.blank?
        raise ArgumentError, 'You must specify a cluster'
      end

      default_options = {:only_leaf => true}
      options.reverse_merge!(default_options)

      cluster = cluster.with_indifferent_access
      cluster_keys.inject(Array.new) do |result, key|
        param = (options[:only_leaf] ? cluster.fetch(key) : cluster[key])
        next result unless cluster.has_key?(key)
        next result << param if result.size >= cluster_keys.index(key)

        raise ArgumentError, 'An incorrectly specified cluster %s' % [cluster]
      end
    end


    # Protected: Возвращает массив партиций, где каждая партиция,
    # представляет собой массив параметров, однозначно её идентифицирующих.
    #
    # part    - Hash - хеш параметров, определяющий партицию.
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
    def prepared_part(parts, options = {})
      default_options = {:only_leaf => false}
      options.reverse_merge!(default_options)

      partition = parts.with_indifferent_access
      partition_keys.inject(Array.new) do |result, key|
        param = (options[:only_leaf] ? partition.fetch(key) : partition[key])
        next result unless partition.has_key?(key)
        next result << param if result.size >= partition_keys.index(key)

        raise ArgumentError, 'An incorrectly specified partition %s' % [partition]
      end
    end

    # Protected: Возвращает массив листовых партиций в виде ключей.
    #
    # params  - Hash - параметров, определяющий кластер и партицию.
    #
    # Если кластер не указан и нет кластеризации в счетчике, то возвращает все партиции.
    # Партиция может быть не задана, тогда будут возвращены все партиции кластера (все партиции, если нет кластеризации).
    # Может быть задана не листовая партиция, тогда будут все её листовые подпартции.
    #
    # Returns Array of Hash.
    #
    def partitions_raw(params = {})
      cluster = prepared_cluster(params)
      partition = prepared_part(params)

      strict_pattern = key(partition, cluster) if (cluster.present? && partition_keys.blank?) || partition.present?
      fuzzy_pattern = key(partition << '*', cluster)

      result = []
      result |= redis.keys(strict_pattern) if strict_pattern.present?
      result |= redis.keys(fuzzy_pattern) if fuzzy_pattern.present?
      result
    end
  end
end