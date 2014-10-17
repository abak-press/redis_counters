require 'spec_helper'

describe RedisCounters::UniqueHashCounter do
  let(:redis) { MockRedis.new }
  let(:unique_list_postfix) { described_class.const_get(:UNIQUE_LIST_POSTFIX) }

  let(:options) { {
      :counter_name => :test_counter,
      :field_name   => :test_field,
      :unique_list  => { :list_class => RedisCounters::UniqueValuesLists::Blocking }
  } }

  let(:counter) { described_class.new(redis, options) }

  context '#partitions' do
    let(:options) { {
        :counter_name => :visits_by_day,
        :field_name => 'dd',
        :partition_keys => [:date],
        :unique_list  => {
            :list_class => RedisCounters::UniqueValuesLists::NonBlocking,
            :value_keys => [:sid],
            :cluster_keys => [:p1],
            :partition_keys => [:p2]
        }
    } }

    before { counter.process(:date => '2013-10-17', :p1 => '2', :p2 => '3', :sid => '1') }

    it { expect(counter.partitions).to have(1).partitions }
    it { expect(counter.partitions.first).to include ({:date => '2013-10-17'}) }
  end

  it { expect(counter).to be_a_kind_of RedisCounters::HashCounter }

  context 'when unique_list not given' do
    let(:options) { {
        :counter_name => :test_counter,
        :field_name   => :test_field
    } }

    it { expect { counter.process }.to raise_error KeyError }
  end

  context 'when only partition_keys and partition_keys given' do
    let(:options) { {
        :counter_name   => :test_counter,
        :field_name     => :test_field,
        :group_keys => [:param1],
        :partition_keys => [:date],
        :unique_list  => {
          :list_class => RedisCounters::UniqueValuesLists::Blocking,
          :value_keys => [:sid],
          :cluster_keys => [:param2],
          :partition_keys => [:date]
        }
    } }

    before { 2.times { counter.process(:param1 => 1, :param2 => 2, :date => '2013-04-27', :sid => 1) } }
    before { 2.times { counter.process(:param1 => 1, :param2 => 2, :date => '2013-04-27', :sid => 2) } }
    before { 2.times { counter.process(:param1 => 2, :param2 => 2, :date => '2013-04-27', :sid => 3) } }
    before { 2.times { counter.process(:param1 => 2, :param2 => 1, :date => '2013-04-28', :sid => 1) } }
    before { 2.times { counter.process(:param1 => 2, :param2 => 1, :date => '2013-04-28', :sid => 5) } }
    before { 2.times { counter.process(:param1 => 2, :param2 => 1, :date => '2013-04-27', :sid => 4) } }
    before { 2.times { counter.process(:param1 => 2, :param2 => 1, :date => '2013-04-27', :sid => 1) } }
    before { 2.times { counter.process(:param1 => 2, :param2 => 1, :date => '2013-04-28', :sid => 4) } }
    before { 2.times { counter.process(:param1 => 2, :param2 => 1, :date => '2013-04-28', :sid => 4) } }
    before { 2.times { counter.process(:param1 => 2, :param2 => 1, :date => '2013-04-27', :sid => 5) } }

    it { expect(redis.keys('*')).to have(7).key }

    it { expect(redis.keys('*')).to include 'test_counter:2013-04-27' }
    it { expect(redis.hget('test_counter:2013-04-27', '1')).to eq 2.to_s }
    it { expect(redis.hget('test_counter:2013-04-27', '2')).to eq 2.to_s }
    it { expect(redis.hget('test_counter:2013-04-28', '2')).to eq 2.to_s }

    it { expect(redis.lrange('test_counter_uq:1:partitions', 0, -1)).to eq ['2013-04-28', '2013-04-27'] }
    it { expect(redis.lrange('test_counter_uq:2:partitions', 0, -1)).to eq ['2013-04-27'] }
    it { expect(redis.smembers('test_counter_uq:1:2013-04-27')).to eq ['4'] }
    it { expect(redis.smembers('test_counter_uq:2:2013-04-27')).to eq ['3', '2', '1'] }
    it { expect(redis.smembers('test_counter_uq:1:2013-04-28')).to eq ['5', '1'] }
  end
end