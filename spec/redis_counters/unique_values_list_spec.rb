require 'spec_helper'

describe RedisCounters::UniqueValuesList do
  let(:redis) { MockRedis.new }
  let(:values) { rand(10) + 1 }
  let(:partitions_list_postfix) { described_class.const_get(:PARTITIONS_LIST_POSTFIX) }

  let(:counter) { described_class.new(redis, options) }

  context 'when value_keys not given' do
    let(:options) { {
        :counter_name => :test_counter
    } }

    it { expect { counter.add }.to raise_error KeyError }
  end

  context 'when unknown value_key given' do
    let(:options) { {
        :counter_name => :test_counter,
        :value_keys   => [:param0, :param1]
    } }

    it { expect { counter.add(:param1 => 1) }.to raise_error KeyError }
  end

  context 'when unknown group_key given' do
    let(:options) { {
        :counter_name => :test_counter,
        :value_keys   => [:param0],
        :group_keys   => [:param1, :param2],
    } }

    it { expect { counter.add(:param0 => 1, :param1 => 2) }.to raise_error KeyError }
  end

  context 'when unknown partition_key given' do
    let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0],
        :partition_keys => [:param1, :param2],
    } }

    it { expect { counter.add(:param0 => 1, :param1 => 2) }.to raise_error KeyError }
  end

  context 'when group and partition keys given' do
    let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :group_keys     => [:param2],
        :partition_keys => [:param3, :param4]
    } }

    before { values.times { counter.process(:param0 => 1, :param1 => 2, :param2 => :group1, :param3 => :part1, :param4 => :part2) } }
    before { values.times { counter.process(:param0 => 2, :param1 => 1, :param2 => :group1, :param3 => :part1, :param4 => :part2) } }
    before { values.times { counter.process(:param0 => 3, :param1 => 2, :param2 => :group1, :param3 => :part2, :param4 => :part2) } }
    before { values.times { counter.process(:param0 => 4, :param1 => 5, :param2 => :group2, :param3 => :part1, :param4 => :part2) } }

    it { expect(redis.keys('*')).to have(5).key }

    context 'when check partitions' do
      it { expect(redis.exists("test_counter:group1:#{partitions_list_postfix}")).to be_true }
      it { expect(redis.exists("test_counter:group2:#{partitions_list_postfix}")).to be_true }

      it { expect(redis.smembers("test_counter:group1:#{partitions_list_postfix}")).to have(2).keys }
      it { expect(redis.smembers("test_counter:group2:#{partitions_list_postfix}")).to have(1).keys }

      it { expect(redis.smembers("test_counter:group1:#{partitions_list_postfix}")).to include 'part1:part2' }
      it { expect(redis.smembers("test_counter:group1:#{partitions_list_postfix}")).to include 'part2:part2' }
      it { expect(redis.smembers("test_counter:group2:#{partitions_list_postfix}")).to include 'part1:part2' }
    end

    context 'when check values' do
      it { expect(redis.exists("test_counter:group1:part1:part2")).to be_true }
      it { expect(redis.exists("test_counter:group1:part2:part2")).to be_true }
      it { expect(redis.exists("test_counter:group2:part1:part2")).to be_true }

      it { expect(redis.smembers("test_counter:group1:part1:part2")).to have(2).keys }
      it { expect(redis.smembers("test_counter:group1:part2:part2")).to have(1).keys }
      it { expect(redis.smembers("test_counter:group2:part1:part2")).to have(1).keys }

      it { expect(redis.smembers("test_counter:group1:part1:part2")).to include '1:2' }
      it { expect(redis.smembers("test_counter:group1:part1:part2")).to include '2:1' }
      it { expect(redis.smembers("test_counter:group1:part2:part2")).to include '3:2' }
      it { expect(redis.smembers("test_counter:group2:part1:part2")).to include '4:5' }
    end
  end

  context 'when group and partition keys no given' do
    let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1]
    } }

    before { values.times { counter.process(:param0 => 1, :param1 => 2) } }
    before { values.times { counter.process(:param0 => 1, :param1 => 2) } }
    before { values.times { counter.process(:param0 => 2, :param1 => 1) } }
    before { values.times { counter.process(:param0 => 3, :param1 => 2) } }

    it { expect(redis.keys('*')).to have(1).key }

    context 'when check values' do
      it { expect(redis.exists("test_counter")).to be_true }
      it { expect(redis.smembers("test_counter")).to have(3).keys }

      it { expect(redis.smembers("test_counter")).to include '1:2' }
      it { expect(redis.smembers("test_counter")).to include '2:1' }
      it { expect(redis.smembers("test_counter")).to include '3:2' }
    end
  end

  context 'when no group keys given, but partition keys given' do
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

      it { expect(redis.smembers("test_counter:#{partitions_list_postfix}")).to have(2).keys }

      it { expect(redis.smembers("test_counter:#{partitions_list_postfix}")).to include 'part1:part2' }
      it { expect(redis.smembers("test_counter:#{partitions_list_postfix}")).to include 'part2:part2' }
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

  context 'when group keys given, but partition keys not given' do
    let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :group_keys     => [:param2]
    } }

    before { values.times { counter.process(:param0 => 1, :param1 => 2, :param2 => :group1) } }
    before { values.times { counter.process(:param0 => 2, :param1 => 1, :param2 => :group1) } }
    before { values.times { counter.process(:param0 => 3, :param1 => 2, :param2 => :group1) } }
    before { values.times { counter.process(:param0 => 4, :param1 => 5, :param2 => :group2) } }

    it { expect(redis.keys('*')).to have(2).key }

    context 'when check values' do
      it { expect(redis.exists("test_counter:group1")).to be_true }
      it { expect(redis.exists("test_counter:group2")).to be_true }

      it { expect(redis.smembers("test_counter:group1")).to have(3).keys }
      it { expect(redis.smembers("test_counter:group2")).to have(1).keys }

      it { expect(redis.smembers("test_counter:group1")).to include '1:2' }
      it { expect(redis.smembers("test_counter:group1")).to include '2:1' }
      it { expect(redis.smembers("test_counter:group1")).to include '3:2' }
      it { expect(redis.smembers("test_counter:group2")).to include '4:5' }
    end
  end

  context 'when block given' do
    let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0]
    } }

    context 'when item added' do
      it { expect { |b| counter.process(:param0 => 1, &b) }.to yield_with_args(redis) }
      it { expect(counter.process(:param0 => 1)).to be_true }
    end

    context 'when item not added' do
      before { counter.process(:param0 => 1) }

      it { expect { |b| counter.process(:param0 => 1, &b) }.to_not yield_with_args(redis) }
      it { expect(counter.process(:param0 => 1)).to be_false }
    end
  end
end