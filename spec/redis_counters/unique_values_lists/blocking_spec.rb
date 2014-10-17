require 'spec_helper'

describe RedisCounters::UniqueValuesLists::Blocking do
  it_behaves_like 'unique_values_lists/common'
  it_behaves_like 'unique_values_lists/set'

  context 'when check partitions list' do
    let(:redis) { MockRedis.new }
    let(:values) { rand(10) + 1 }
    let(:partitions_list_postfix) { described_class.const_get(:PARTITIONS_LIST_POSTFIX) }

    let(:counter) { described_class.new(redis, options) }

    context 'when group and partition keys given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :cluster_keys   => [:param2],
        :partition_keys => [:param3, :param4]
      } }

      before { values.times { counter.process(:param0 => 1, :param1 => 2, :param2 => :cluster1, :param3 => :part1, :param4 => :part2) } }
      before { values.times { counter.process(:param0 => 2, :param1 => 1, :param2 => :cluster1, :param3 => :part1, :param4 => :part2) } }
      before { values.times { counter.process(:param0 => 3, :param1 => 2, :param2 => :cluster1, :param3 => :part2, :param4 => :part2) } }
      before { values.times { counter.process(:param0 => 4, :param1 => 5, :param2 => :cluster2, :param3 => :part1, :param4 => :part2) } }

      it { expect(redis.keys('*')).to have(5).key }

      context 'when check partitions' do
        it { expect(redis.lrange("test_counter:cluster1:#{partitions_list_postfix}", 0, -1)).to be_true }
        it { expect(redis.lrange("test_counter:cluster2:#{partitions_list_postfix}", 0, -1)).to be_true }

        it { expect(redis.lrange("test_counter:cluster1:#{partitions_list_postfix}", 0, -1)).to have(2).keys }
        it { expect(redis.lrange("test_counter:cluster2:#{partitions_list_postfix}", 0, -1)).to have(1).keys }

        it { expect(redis.lrange("test_counter:cluster1:#{partitions_list_postfix}", 0, -1)).to include 'part1:part2' }
        it { expect(redis.lrange("test_counter:cluster1:#{partitions_list_postfix}", 0, -1)).to include 'part2:part2' }
        it { expect(redis.lrange("test_counter:cluster2:#{partitions_list_postfix}", 0, -1)).to include 'part1:part2' }
      end

      context 'when check values' do
        it { expect(redis.exists("test_counter:cluster1:part1:part2")).to be_true }
        it { expect(redis.exists("test_counter:cluster1:part2:part2")).to be_true }
        it { expect(redis.exists("test_counter:cluster2:part1:part2")).to be_true }

        it { expect(redis.smembers("test_counter:cluster1:part1:part2")).to have(2).keys }
        it { expect(redis.smembers("test_counter:cluster1:part2:part2")).to have(1).keys }
        it { expect(redis.smembers("test_counter:cluster2:part1:part2")).to have(1).keys }

        it { expect(redis.smembers("test_counter:cluster1:part1:part2")).to include '1:2' }
        it { expect(redis.smembers("test_counter:cluster1:part1:part2")).to include '2:1' }
        it { expect(redis.smembers("test_counter:cluster1:part2:part2")).to include '3:2' }
        it { expect(redis.smembers("test_counter:cluster2:part1:part2")).to include '4:5' }
      end
    end

    context 'when no cluster keys given, but partition keys given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :partition_keys => [:param3, :param4]
      } }

      before { values.times { counter.process(:param0 => 1, :param1 => 2, :param3 => :part1, :param4 => :part2) } }
      before { values.times { counter.process(:param0 => 2, :param1 => 1, :param3 => :part1, :param4 => :part2) } }
      before { values.times { counter.process(:param0 => 3, :param1 => 2, :param3 => :part2, :param4 => :part2) } }
      before { values.times { counter.process(:param0 => 4, :param1 => 5, :param3 => :part1, :param4 => :part2) } }

      it { expect(redis.keys('*')).to have(3).key }

      context 'when check partitions' do
        it { expect(redis.exists("test_counter:#{partitions_list_postfix}")).to be_true }

        it { expect(redis.lrange("test_counter:#{partitions_list_postfix}", 0, -1)).to have(2).keys }

        it { expect(redis.lrange("test_counter:#{partitions_list_postfix}", 0, -1)).to include 'part1:part2' }
        it { expect(redis.lrange("test_counter:#{partitions_list_postfix}", 0, -1)).to include 'part2:part2' }
      end

      context 'when check values' do
        it { expect(redis.exists("test_counter:part1:part2")).to be_true }
        it { expect(redis.exists("test_counter:part2:part2")).to be_true }

        it { expect(redis.smembers("test_counter:part1:part2")).to have(3).keys }
        it { expect(redis.smembers("test_counter:part2:part2")).to have(1).keys }

        it { expect(redis.smembers("test_counter:part1:part2")).to include '1:2' }
        it { expect(redis.smembers("test_counter:part1:part2")).to include '2:1' }
        it { expect(redis.smembers("test_counter:part2:part2")).to include '3:2' }
        it { expect(redis.smembers("test_counter:part1:part2")).to include '4:5' }
      end
    end
  end
end