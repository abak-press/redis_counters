require 'spec_helper'

describe RedisCounters::UniqueValuesLists::NonBlocking do
  it_behaves_like 'unique_values_lists/common'
  it_behaves_like 'unique_values_lists/set'
end