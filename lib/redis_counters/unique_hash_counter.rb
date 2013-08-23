# coding: utf-8
require 'redis_counters/hash_counter'
require 'redis_counters/unique_values_list'

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
      @unique_values_list = UniqueValuesList.new(
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
  end

end