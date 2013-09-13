# coding: utf-8
require 'forwardable'
require 'active_support/core_ext/class/attribute'

module RedisCounters

  # Базовый класс счетчика на основе Redis.

  class BaseCounter
    extend Forwardable

    KEY_DELIMITER = ':'.freeze
    VALUE_DELIMITER = ':'.freeze

    class_attribute :key_delimiter
    class_attribute :value_delimiter

    self.key_delimiter = KEY_DELIMITER
    self.value_delimiter = VALUE_DELIMITER

    attr_reader :redis
    attr_reader :options
    attr_reader :params

    # Public: Фабричный метод создания счетчика заданного класса.
    #
    # redis - Redis - экземпляр redis - клиента.
    # opts - Hash - хеш опций счетчика:
    #        counter_name - Symbol/String - идентификатор счетчика.
    #        key_delimiter - String - разделитель ключа (опционально).
    #        value_delimiter - String - разделитель значений (опционально).
    #
    # Returns RedisCounters::BaseCounter.
    #
    def self.create(redis, opts)
      counter_class = opts.fetch(:counter_class).to_s.constantize
      counter_class.new(redis, opts)
    end

    # Public: Конструктор.
    #
    # см. self.create.
    #
    # Returns RedisCounters::BaseCounter.
    #
    def initialize(redis, opts)
      @redis = redis
      @options = opts
      init
    end

    # Public: Метод производит обработку события.
    #
    # params - Hash - хеш параметров события.
    #
    # Returns process_value result.
    #
    def process(params = {}, &block)
      @params = params
      process_value(&block)
    end

    protected

    def init
      counter_name.present?
    end

    def counter_name
      @counter_name ||= options.fetch(:counter_name)
    end

    def key_delimiter
      @key_delimiter ||= options.fetch(:key_delimiter, self.class.key_delimiter)
    end

    def value_delimiter
      @value_delimiter ||= options.fetch(:value_delimiter, self.class.value_delimiter)
    end

    def_delegator :redis, :multi, :transaction
  end

end