# coding: utf-8
require 'redis_counters/hash_counter'

module RedisCounters

  # HashCounter, с возможностью подсчета только уникальных значений.

  class UniqueHashCounter < HashCounter
    UNIQUE_LIST_POSTFIX = 'uq'.freeze

    UNIQUE_LIST_POSTFIX_DELIMITER = '_'.freeze

    attr_reader :unique_values_list

    protected

    def process_value
      unique_values_list.add(params) { super }
    end

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
      [counter_name, UNIQUE_LIST_POSTFIX].join(unique_list_postfix_delimiter)
    end

    def unique_values_list_class
      unique_values_list_options.fetch(:list_class).to_s.constantize
    end

    def unique_list_postfix_delimiter
      @unique_list_postfix_delimiter ||= options.fetch(:unique_list_postfix_delimiter, UNIQUE_LIST_POSTFIX_DELIMITER)
    end

    def partitions_raw(parts = {})
      # удаляем из списка партиций, ключи в которых хранятся списки уникальных значений
      super.delete_if { |partition| partition.start_with?(unique_values_list_name) }
    end
  end

end