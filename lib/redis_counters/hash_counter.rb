# coding: utf-8
require 'redis_counters/base_counter'

module RedisCounters

  class HashCounter < BaseCounter
    alias_method :increment, :process

    def init
      super
      return if field_name.present? || group_keys.present?
      raise ArgumentError, 'field_name or group_keys required!'
    end

    protected

    def process_value
      redis.hincrby(key, field, 1)
    end

    def key
      [counter_name, partition].flatten.join(key_delimiter)
    end

    def partition
      partition_keys.map do |key|
        key.respond_to?(:call) ? key.call(params) : params.fetch(key)
      end
    end

    def field
      group_params = group_keys.map { |key| params.fetch(key) }
      group_params << field_name if field_name.present?
      group_params.join(value_delimiter)
    end

    def field_name
      @field_name ||= options[:field_name]
    end

    def group_keys
      @group_keys ||= Array.wrap(options.fetch(:group_keys, []))
    end

    def partition_keys
      @partition_keys ||= Array.wrap(options.fetch(:partition_keys, []))
    end
  end

end