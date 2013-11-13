# coding: utf-8
module RedisCounters
  module ClusterizeAndPartitionize
    # Public: Возвращает массив партиций (подпартиций) кластера в виде хешей.
    #
    # Если партиция не указана, возвращает все партиции кластера.
    #
    # cluster - Hash - кластер (опционально, если не используются кластеризация).
    # parts   - Array of Hash - список партиций (опционально).
    #           По умолчанию, выбираются все данные кластера.
    #
    # Returns Array Of Hash.
    #
    def partitions(cluster = {}, parts = {})
      partitions_raw(cluster, parts).map do |part|
        # parse and exclude counter_name and cluster
        part = part.split(key_delimiter, -1).from(1).from(cluster_keys.size)
        # construct hash
        Hash[partition_keys.zip(part)].with_indifferent_access
      end
    end

    # Public: Возвращает данные счетчика для указанной кластера из указанных партиций.
    #
    # cluster - Hash - кластер (опционально, если не используются кластеризация).
    # parts   - Array of Hash - список партиций (опционально).
    #           По умолчанию, выбираются все данные кластера.
    #
    # Если передан блок, то вызывает блок для каждой партиции.
    # Если блок, не передн, то аккумулирует данные,
    # из всех запрошенных партиций, и затем возвращает их.
    #
    # Returns Array Of Hash.
    #
    def data(cluster = {}, parts = {})
      total_rows = 0
      params = cluster.merge(parts)
      parts = partitions(cluster, parts).map { |partition| prepared_parts(partition) }
      cluster = prepared_cluster(params)

      result = parts.flat_map do |partition|
        rows = partition_data(cluster, partition)
        total_rows += rows.size
        block_given? ? yield(rows) : rows
      end

      block_given? ? total_rows : result
    end

    # Public: Транзакционно удаляет все данные счетчика.
    #
    # cluster - Hash - кластер (опционально, если не используются кластеризация).
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

    # Public: Транзакционно удаляет данные всех указанных партиций.
    #
    # parts - Array of Hash - список партиций.
    #
    # Если передан блок, то вызывает блок, после удаления всех данных, в транзакции.
    #
    # Returns Nothing.
    #
    def delete_partitions!(cluster, parts)
      if parts.blank?
        raise ArgumentError, 'You must specify a partitions'
      end

      parts = partitions(cluster, parts)

      transaction do
        parts.each { |partition| delete_partition_direct!(cluster, partition) }
        yield if block_given?
      end
    end

    # Public: Нетранзакционно удаляет все данные счетчика.
    #
    # cluster       - Hash - кластер.
    # write_session - Redis - соединение с Redis, в рамках которого
    #                 будет производится удаление (опционально).
    #                 По умолчанию - основное соединение счетчика.
    #
    # Returns Nothing.
    #
    def delete_all_direct!(cluster, write_session = redis, parts = partitions(cluster))
      parts.each { |partition| delete_partition_direct!(cluster, partition, write_session) }
    end

    # Public: Нетранзакционно удаляет данные конкретной конечной партиции.
    #
    # cluster       - Hash - кластер.
    # partition     - Hash - партиция.
    # write_session - Redis - соединение с Redis, в рамках которого
    #                 будет производится удаление (опционально).
    #                 По умолчанию - основное соединение счетчика.
    #
    # Returns Nothing.
    #
    def delete_partition_direct!(cluster, partition = {}, write_session = redis)
      cluster = prepared_cluster(cluster)
      partition = prepared_parts(partition, :only_leaf => true)
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
    # cluster - Hash - кластер.
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
    # Если кластер не указан и нет кластеризации в счетчике, то возвращает все партиции.
    # Если партиция не указана, возвращает все партиции кластера (все партиции, если нет кластеризации).
    #
    # cluster - Hash - кластер.
    # parts   - Array of Hash - список партиций.
    #
    # Returns Array of Hash.
    #
    def partitions_raw(cluster = {}, parts = {})
      params = cluster.merge(parts)
      cluster = prepared_cluster(params)
      partition = prepared_parts(params)
      strict_pattern = key(partition, cluster) if (cluster.present? && partition_keys.blank?) || partition.present?
      fuzzy_pattern = key(partition << '*', cluster)
      result = []
      result |= redis.keys(strict_pattern) if strict_pattern.present?
      result |= redis.keys(fuzzy_pattern) if fuzzy_pattern.present?
      result
    end
  end
end