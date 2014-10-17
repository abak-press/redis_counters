require 'spec_helper'

describe RedisCounters::UniqueValuesLists::Expirable do
  it_behaves_like 'unique_values_lists/common'
  it_behaves_like 'unique_values_lists/expirable'
end