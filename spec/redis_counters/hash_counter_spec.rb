require 'spec_helper'

describe RedisCounters::HashCounter do
  let(:redis) { MockRedis.new }
  let(:value) { rand(10) + 1 }
  let(:options) { { :counter_name => :test_counter, :field_name => :test_field } }
  let(:counter) { described_class.new(redis, options) }

  it { expect(counter).to respond_to :process }
  it { expect(counter).to respond_to :increment }

  context 'when field_name and group_keys not given' do
    let(:options) { { :counter_name => :test_counter } }

    it { expect { counter.process }.to raise_error ArgumentError }
  end

  context 'when only field_name given' do
    let(:options) { {
        :counter_name => :test_counter,
        :field_name   => :test_field
    } }

    before { value.times { counter.process } }

    it { expect(redis.keys('*')).to have(1).key }
    it { expect(redis.keys('*').first).to eq 'test_counter' }
    it { expect(redis.hexists('test_counter', 'test_field')).to be_true }
    it { expect(redis.hget('test_counter', 'test_field')).to eq value.to_s }
  end

  context 'when group_keys given' do
    context 'when field_name not given' do
      let(:options) { {
          :counter_name => :test_counter,
          :group_keys   => [:param1, :param2]
      } }

      before { value.times { counter.process(:param1 => 11, :param2 => 22, :param3 => 33) } }

      it { expect(redis.keys('*')).to have(1).key }
      it { expect(redis.keys('*').first).to eq 'test_counter' }
      it { expect(redis.hexists('test_counter', '11:22')).to be_true }
      it { expect(redis.hget('test_counter', '11:22')).to eq value.to_s }
    end

    context 'when exists group_keys given' do
      let(:options) { {
          :counter_name => :test_counter,
          :field_name   => :test_field,
          :group_keys   => [:param1, :param2]
      } }

      before { value.times { counter.process(:param1 => 11, :param2 => 22, :param3 => 33) } }
      before { 2.times { counter.process(:param1 => 12, :param2 => 22, :param3 => 33) } }

      it { expect(redis.keys('*')).to have(1).key }
      it { expect(redis.keys('*').first).to eq 'test_counter' }
      it { expect(redis.hexists('test_counter', '11:22:test_field')).to be_true }
      it { expect(redis.hget('test_counter', '11:22:test_field')).to eq value.to_s }
      it { expect(redis.hexists('test_counter', '12:22:test_field')).to be_true }
      it { expect(redis.hget('test_counter', '12:22:test_field')).to eq 2.to_s }
    end

    context 'when not exists group_keys given' do
      let(:options) { {
          :counter_name => :test_counter,
          :field_name   => :test_field,
          :group_keys   => [:param1, :param4]
      } }

      it { expect { counter.process }.to raise_error KeyError }
    end
  end

  context 'when use partition' do
    context 'when all partition_keys is Symbols' do
      let(:options) { {
          :counter_name   => :test_counter,
          :field_name     => :test_field,
          :partition_keys => [:param1, :param2]
      } }

      before { value.times { counter.process(:param1 => 11, :param2 => 22, :param3 => 33) } }
      before { 3.times { counter.process(:param1 => 21, :param2 => 22, :param3 => 33) } }

      it { expect(redis.keys('*')).to have(2).key }
      it { expect(redis.keys('*').first).to eq 'test_counter:11:22' }
      it { expect(redis.keys('*').last).to eq 'test_counter:21:22' }
      it { expect(redis.hexists('test_counter:11:22', 'test_field')).to be_true }
      it { expect(redis.hget('test_counter:11:22', 'test_field')).to eq value.to_s }
      it { expect(redis.hexists('test_counter:21:22', 'test_field')).to be_true }
      it { expect(redis.hget('test_counter:21:22', 'test_field')).to eq 3.to_s }
    end

    context 'when all partition_keys is Proc' do
      let(:options) { {
          :counter_name   => :test_counter,
          :field_name     => :test_field,
          :partition_keys => proc { |params| params[:param1].odd?.to_s }
      } }

      before { 2.times { counter.process(:param1 => 1, :param2 => 2) } }
      before { 3.times { counter.process(:param1 => 2, :param2 => 2) } }

      it { expect(redis.keys('*')).to have(2).key }
      it { expect(redis.keys('*').first).to eq 'test_counter:true' }
      it { expect(redis.keys('*').last).to eq 'test_counter:false' }
      it { expect(redis.hexists('test_counter:true', 'test_field')).to be_true }
      it { expect(redis.hget('test_counter:true', 'test_field')).to eq 2.to_s }
      it { expect(redis.hexists('test_counter:false', 'test_field')).to be_true }
      it { expect(redis.hget('test_counter:false', 'test_field')).to eq 3.to_s }
    end

    context 'when partition_keys consists of mixed types' do
      let(:options) { {
          :counter_name   => :test_counter,
          :field_name     => :test_field,
          :partition_keys => [:date, proc { |params| params[:param1].odd?.to_s }]
      } }

      before { 2.times { counter.process(:param1 => 1, :param2 => 2, :date => '2013-04-27') } }
      before { 1.times { counter.process(:param1 => 3, :param2 => 2, :date => '2013-04-27') } }
      before { 4.times { counter.process(:param1 => 2, :param2 => 2, :date => '2013-04-27') } }
      before { 1.times { counter.process(:param1 => 2, :param2 => 2, :date => '2013-04-28') } }

      it { expect(redis.keys('*')).to have(3).key }
      it { expect(redis.keys('*').first).to eq 'test_counter:2013-04-27:true' }
      it { expect(redis.keys('*').second).to eq 'test_counter:2013-04-27:false' }
      it { expect(redis.keys('*').third).to eq 'test_counter:2013-04-28:false' }
      it { expect(redis.hexists('test_counter:2013-04-27:true', 'test_field')).to be_true }
      it { expect(redis.hget('test_counter:2013-04-27:true', 'test_field')).to eq 3.to_s }
      it { expect(redis.hexists('test_counter:2013-04-27:false', 'test_field')).to be_true }
      it { expect(redis.hget('test_counter:2013-04-27:false', 'test_field')).to eq 4.to_s }
      it { expect(redis.hexists('test_counter:2013-04-27:false', 'test_field')).to be_true }
      it { expect(redis.hget('test_counter:2013-04-28:false', 'test_field')).to eq 1.to_s }
    end
  end
end
