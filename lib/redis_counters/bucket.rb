# coding: utf-8
module RedisCounters

  class Bucket

    def self.default_options
      {:only_leaf => false}
    end

    def initialize(counter, bucket_params)
      @counter = counter
      @bucket_params = bucket_params.with_indifferent_access

      if bucket_keys.present? && bucket_params.blank? && required?
        raise ArgumentError, "You must specify a #{self.class.name}"
      end
    end

    attr_reader :counter
    attr_reader :bucket_params

    # Protected: Возвращает букет в виде массива параметров, однозначно его идентифицирующих.
    #
    # cluster - Hash - хеш параметров, определяющий букет.
    # options - Hash - хеш опций:
    #           :only_leaf - Boolean - выбирать только листовые букеты (по умолачнию - true).
    #                        Если флаг установлен в true и передана не листовой букет, то
    #                        будет сгенерировано исключение KeyError.
    #
    # Метод генерирует исключение ArgumentError, если переданы параметры не верно идентифицирующие букет.
    # Например: ключи группировки счетчика {:param1, :param2, :param3}, а переданы {:param1, :param3}.
    # Метод генерирует исключение ArgumentError, 'You must specify a cluster',
    # если букет передан в виде пустого хеша, но группировка используется в счетчике.
    #
    # Returns Array.
    #
    def params(options = {})
      options.reverse_merge!(self.class.default_options)

      bucket_keys.inject(Array.new) do |result, key|
        param = (options[:only_leaf] ? bucket_params.fetch(key) : bucket_params[key])
        next result unless bucket_params.has_key?(key)
        next result << param if result.size >= bucket_keys.index(key)

        raise ArgumentError, 'An incorrectly specified %s %s' % [self.class.name, bucket_params]
      end
    end

    protected

    def bucket_keys
      raise NotImplementedError.new 'You must specify the grouping key'
    end

    def required?
      false
    end
  end
end