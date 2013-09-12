# coding: utf-8
require 'redis_counters/hash_counter'

module RedisCounters

  class UniqueHashCounter < HashCounter
    UNIQUE_LIST_POSTFIX = 'uq'.freeze

    protected

    def process_value
      unique_values_list.add(params) { super }
    end

    attr_reader :unique_values_list

    def init
      super
      @unique_values_list = unique_values_list_class.new(
        redis,
        unique_values_list_options
      )
    end

    def unique_values_list_options
      options.fetch(:unique_list).merge!(:counter_name => unique_values_list_name)
    end

    def unique_values_list_name
      [counter_name, UNIQUE_LIST_POSTFIX].join(KEY_DELIMITER)
    end

    def unique_values_list_class
      unique_values_list_options.fetch(:list_class).to_s.constantize
    end
  end

end