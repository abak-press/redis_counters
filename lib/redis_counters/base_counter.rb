# coding: utf-8
require 'forwardable'

module RedisCounters

  class BaseCounter
    extend Forwardable

    KEY_DELIMITER = ':'.freeze

    attr_reader :redis
    attr_reader :options
    attr_reader :params

    def self.create(redis, opts)
      counter_class = opts.fetch(:counter_class).to_s.constantize
      counter_class.new(redis, opts)
    end

    def initialize(redis, opts)
      @redis = redis
      @options = opts
      init
    end

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

    def_delegator :redis, :multi, :transaction
  end

end