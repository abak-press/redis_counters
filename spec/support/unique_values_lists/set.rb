# coding: utf-8
shared_examples_for 'unique_values_lists/set' do
  let(:redis) { MockRedis.new }
  let(:values) { rand(10) + 1 }

  let(:options) { {
    :counter_name => :test_counter,
    :value_keys   => [:param0]
  } }

  let(:counter) { described_class.new(redis, options) }

  context '#add' do
    context 'when cluster and partition keys given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :cluster_keys     => [:param2],
        :partition_keys => [:param3, :param4]
      } }

      before { values.times { counter.add(:param0 => 1, :param1 => 2, :param2 => :cluster1, :param3 => :part1, :param4 => :part2) } }
      before { values.times { counter.add(:param0 => 2, :param1 => 1, :param2 => :cluster1, :param3 => :part1, :param4 => :part2) } }
      before { values.times { counter.add(:param0 => 3, :param1 => 2, :param2 => :cluster1, :param3 => :part2, :param4 => :part2) } }
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :param2 => :cluster2, :param3 => :part1, :param4 => :part2) } }

      it { expect(redis.keys('*')).to have(5).key }

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

    context 'when cluster and partition keys no given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1]
      } }

      before { values.times { counter.add(:param0 => 1, :param1 => 2) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 2) } }
      before { values.times { counter.add(:param0 => 2, :param1 => 1) } }
      before { values.times { counter.add(:param0 => 3, :param1 => 2) } }

      it { expect(redis.keys('*')).to have(1).key }

      it { expect(redis.exists("test_counter")).to be_true }
      it { expect(redis.smembers("test_counter")).to have(3).keys }

      it { expect(redis.smembers("test_counter")).to include '1:2' }
      it { expect(redis.smembers("test_counter")).to include '2:1' }
      it { expect(redis.smembers("test_counter")).to include '3:2' }
    end

    context 'when no cluster keys given, but partition keys given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :partition_keys => [:param3, :param4]
      } }

      before { values.times { counter.add(:param0 => 1, :param1 => 2, :param3 => :part1, :param4 => :part2) } }
      before { values.times { counter.add(:param0 => 2, :param1 => 1, :param3 => :part1, :param4 => :part2) } }
      before { values.times { counter.add(:param0 => 3, :param1 => 2, :param3 => :part2, :param4 => :part2) } }
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :param3 => :part1, :param4 => :part2) } }

      it { expect(redis.keys('*')).to have(3).key }

      it { expect(redis.exists("test_counter:part1:part2")).to be_true }
      it { expect(redis.exists("test_counter:part2:part2")).to be_true }

      it { expect(redis.smembers("test_counter:part1:part2")).to have(3).keys }
      it { expect(redis.smembers("test_counter:part2:part2")).to have(1).keys }

      it { expect(redis.smembers("test_counter:part1:part2")).to include '1:2' }
      it { expect(redis.smembers("test_counter:part1:part2")).to include '2:1' }
      it { expect(redis.smembers("test_counter:part2:part2")).to include '3:2' }
      it { expect(redis.smembers("test_counter:part1:part2")).to include '4:5' }
    end

    context 'when cluster keys given, but partition keys not given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :cluster_keys     => [:param2]
      } }

      before { values.times { counter.add(:param0 => 1, :param1 => 2, :param2 => :cluster1) } }
      before { values.times { counter.add(:param0 => 2, :param1 => 1, :param2 => :cluster1) } }
      before { values.times { counter.add(:param0 => 3, :param1 => 2, :param2 => :cluster1) } }
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :param2 => :cluster2) } }

      it { expect(redis.keys('*')).to have(2).key }

      it { expect(redis.exists("test_counter:cluster1")).to be_true }
      it { expect(redis.exists("test_counter:cluster2")).to be_true }

      it { expect(redis.smembers("test_counter:cluster1")).to have(3).keys }
      it { expect(redis.smembers("test_counter:cluster2")).to have(1).keys }

      it { expect(redis.smembers("test_counter:cluster1")).to include '1:2' }
      it { expect(redis.smembers("test_counter:cluster1")).to include '2:1' }
      it { expect(redis.smembers("test_counter:cluster1")).to include '3:2' }
      it { expect(redis.smembers("test_counter:cluster2")).to include '4:5' }
    end
  end
end