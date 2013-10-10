require 'spec_helper'

describe RedisCounters::HashCounter do
  let(:redis) { MockRedis.new }
  let(:value) { rand(10) + 1 }
  let(:options) { { :counter_name => :test_counter, :field_name => :test_field } }
  let(:counter) { described_class.new(redis, options) }

  context 'when check interface' do
    it { expect(counter).to respond_to :process }
    it { expect(counter).to respond_to :increment }
    it { expect(counter).to respond_to :partitions }
    it { expect(counter).to respond_to :partitions_raw }
    it { expect(counter).to respond_to :data }
    it { expect(counter).to respond_to :delete_all! }
    it { expect(counter).to respond_to :delete_partitions! }
    it { expect(counter).to respond_to :delete_partition_direct! }
  end

  context 'when field_name and group_keys not given' do
    let(:options) { { :counter_name => :test_counter } }

    it { expect { counter.process }.to raise_error KeyError }
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
      it { expect(redis.hexists('test_counter', '11:22')).to be_true }
      it { expect(redis.hget('test_counter', '11:22')).to eq value.to_s }
      it { expect(redis.hexists('test_counter', '12:22')).to be_true }
      it { expect(redis.hget('test_counter', '12:22')).to eq 2.to_s }
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

  context '#partitions' do
    let(:options) { {
        :counter_name   => :test_counter,
        :field_name     => :test_field,
        :partition_keys => [:param1, :param2, :param3]
    } }

    let(:partition_1) { {'param1' => '11', :param2 => '22', :param3 => '33'}.with_indifferent_access }
    let(:partition_2) { {'param1' => '21', :param2 => '23', :param3 => '33'}.with_indifferent_access }
    let(:partition_3) { {'param1' => '21', :param2 => '24', :param3 => '33'}.with_indifferent_access }

    before { counter.process(partition_1) }
    before { counter.process(partition_2) }
    before { counter.process(partition_3) }

    context 'when no partition params given' do
      it { expect(counter.partitions).to have(3).partition }
      it { expect(counter.partitions.first).to eq partition_1 }
      it { expect(counter.partitions.second).to eq partition_2 }
      it { expect(counter.partitions.third).to eq partition_3 }
    end

    context 'when one partition param given' do
      it { expect(counter.partitions(:param1 => 11)).to have(1).partition }
      it { expect(counter.partitions(:param1 => 11).first).to eq partition_1 }
      it { expect(counter.partitions(:param1 => 21)).to have(2).partition }
      it { expect(counter.partitions(:param1 => 21).first).to eq partition_2 }
      it { expect(counter.partitions('param1' => 21).second).to eq partition_3 }
    end

    context 'when two partition params given' do
      it { expect(counter.partitions(:param1 => 21, :param2 => 23)).to have(1).partition }
      it { expect(counter.partitions(:param1 => 21, :param2 => 23).first).to eq partition_2 }
    end

    context 'when all partition params given' do
      it { expect(counter.partitions(partition_2)).to have(1).partition }
      it { expect(counter.partitions(partition_2).first).to eq partition_2 }
    end

    context 'when unknown partition params given' do
      it { expect(counter.partitions(:param1 => 55)).to eq [] }
    end

    context 'when given an incorrect partition' do
      it { expect { counter.partitions(:param2 => 55) }.to raise_error ArgumentError }
    end

    context 'when array of partitions given' do
      it { expect(counter.partitions([{:param1 => 11}, partition_2])).to have(2).partition }
      it { expect(counter.partitions([{:param1 => 11}, partition_2]).first).to eq partition_1 }
      it { expect(counter.partitions([{:param1 => 11}, partition_2]).second).to eq partition_2 }
    end
  end

  context '#data' do
    context 'when no group_keys given' do
      let(:options) { {
          :counter_name => :pages_by_day,
          :field_name   => :pages,
          :partition_keys => [:param1, :param2]
      } }

      let(:partitions) { {:param1 => 21} }

      before { value.times { counter.process(:param1 => 11, :param2 => 22, :param3 => 33) } }
      before { 3.times { counter.process(:param1 => 21, :param2 => 22, :param3 => 33) } }
      before { 2.times { counter.process(:param1 => 21, :param2 => 23, :param3 => 31) } }

      it { expect(counter.data(partitions)).to have(2).row }
      it { expect(counter.data(partitions).first[:value]).to eq 3 }
      it { expect(counter.data(partitions).second[:value]).to eq 2 }
    end

    context 'when group_keys given' do
      let(:options) { {
          :counter_name   => :test_counter,
          :field_name     => :test_field,
          :partition_keys => [:param1, :param2],
          :group_keys     => :param3
      } }

      before { value.times { counter.process(:param1 => 11, :param2 => 22, :param3 => 33) } }
      before { 3.times { counter.process(:param1 => 21, :param2 => 22, :param3 => 33) } }
      before { 2.times { counter.process(:param1 => 21, :param2 => 22, :param3 => 31) } }

      context 'when partition as Hash_given' do
        let(:partitions) { {:param1 => 21, 'param3' => 33, :param2 => 22} }

        it { expect(counter.data(partitions)).to be_a Array }
        it { expect(counter.data(partitions)).to have(2).row }
        it { expect(counter.data(partitions).first).to be_a HashWithIndifferentAccess }
        it { expect(counter.data(partitions).first).to include('value') }
        it { expect(counter.data(partitions).first).to include(:value) }
        it { expect(counter.data(partitions).first).to include(:param3) }
        it { expect(counter.data(partitions).first[:value]).to eq 3 }
        it { expect(counter.data(partitions).first[:param3]).to eq '33' }
        it { expect(counter.data(partitions).second[:value]).to eq 2 }
        it { expect(counter.data(partitions).second[:param3]).to eq '31' }
      end

      context 'when few partition_given' do
        let(:partitions) do
          [
            {:param1 => 21, :param3 => 33, 'param2' => '22'},
            {'param1' => '11', :param3 => 33, :param2 => 22}
          ]
        end

        it { expect(counter.data(partitions)).to be_a Array }
        it { expect(counter.data(partitions)).to have(3).row }
        it { expect(counter.data(partitions).first).to be_a HashWithIndifferentAccess }
        it { expect(counter.data(partitions).first).to include('value') }
        it { expect(counter.data(partitions).first).to include(:value) }
        it { expect(counter.data(partitions).first[:value]).to eq 3 }
        it { expect(counter.data(partitions).second[:value]).to eq 2 }
        it { expect(counter.data(partitions).third[:'value']).to eq value }
      end

      context 'when unknown partition_given' do
        let(:partitions) do
          [
              {:param1 => 22, :param3 => 33, 'param2' => '22'},
              {'param1' => '11', :param3 => 33, :param2 => 22}
          ]
        end

        it { expect(counter.data(partitions)).to have(1).row }
        it { expect(counter.data(partitions).first[:value]).to eq value }
        it { expect(counter.data(partitions).first[:param3]).to eq '33' }
      end

      context 'when no data in storage' do
        let(:partitions) { {:param1 => 21, 'param3' => 33, :param2 => 22} }

        before { redis.flushdb }

        it { expect(counter.data(partitions)).to be_a Array }
        it { expect(counter.data(partitions)).to be_empty }
      end


      context 'when block given' do
        let(:partitions) do
          [
              {:param1 => 21, :param3 => 33, 'param2' => '22'},
              {'param1' => '11', :param3 => 33, :param2 => 22}
          ]
        end

        it { expect { |b| counter.data(partitions, &b) }.to yield_control.twice }
        it do
          expect { |b| counter.data(partitions, &b) }.to(
            yield_successive_args(
              [{'param3' => '33', 'value' => 3}, {'param3' => '31', 'value' => 2}],
              [{'param3'=>'33', 'value'=>value}]
            )
          )
        end
      end
    end
  end

  context 'when check deleting data methods' do
    let(:options) { {
        :counter_name   => :test_counter,
        :field_name     => :test_field,
        :partition_keys => [:param1, :param2, :param3]
    } }

    let(:partition_1) { {'param1' => '11', :param2 => '22', :param3 => '33'}.with_indifferent_access }
    let(:partition_2) { {'param1' => '21', :param2 => '23', :param3 => '33'}.with_indifferent_access }
    let(:partition_3) { {'param1' => '21', :param2 => '24', :param3 => '31'}.with_indifferent_access }

    before { counter.process(partition_1) }
    before { counter.process(partition_2) }
    before { counter.process(partition_3) }

    context '#delete_all!' do
      before { counter.delete_all! }

      it { expect(counter.partitions).to eq [] }
      it { expect(counter.data).to eq [] }
      it { expect(redis.keys).to eq [] }
    end

    context '#delete_partitions!' do
      context 'when leaf partition given' do
        before { counter.delete_partitions!(partition_2) }

        it { expect(counter.partitions).to have(2).row }
        it { expect(counter.partitions.first).to eq partition_1 }
        it { expect(counter.partitions.last).to eq partition_3 }
      end

      context 'when not leaf partition given' do
        before { counter.delete_partitions!(:param1 => 21) }

        it { expect(counter.partitions).to have(1).row }
        it { expect(counter.partitions.first).to eq partition_1 }
      end

      context 'when block given' do
        it { expect { |b| counter.delete_partitions!(:param1 => 21, &b) }.to yield_control.once }
      end

      context 'if you pass a block in which the exception occurred' do
        let(:error_proc) { Proc.new { raise '!' } }

        before { counter.delete_partitions!(:param1 => 21, &error_proc) rescue nil }

        it { expect(counter.partitions).to have(3).row }
        it { expect(counter.partitions.first).to eq partition_1 }
        it { expect(counter.partitions.second).to eq partition_2 }
        it { expect(counter.partitions.last).to eq partition_3 }
      end
    end

    context '#delete_partition_direct!' do
      context 'when leaf partition given' do
        before { counter.delete_partitions!(partition_2) }

        it { expect(counter.partitions).to have(2).row }
        it { expect(counter.partitions.first).to eq partition_1 }
        it { expect(counter.partitions.last).to eq partition_3 }
      end

      context 'when not leaf partition given' do
        it { expect { counter.delete_partition_direct!(:param1 => 21) }.to raise_error KeyError }
      end
    end
  end

  context 'when check delimiters' do
    let(:options) { {
        :counter_name    => :test_counter,
        :group_keys      => [:param3],
        :partition_keys  => [:param1, :param2],
        :key_delimiter   => '&',
        :value_delimiter => '|'
    } }

    let(:partition_1) { {:param1 => '11', :param2 => '22:35', :param3 => '11:64'} }
    let(:partition_2) { {:param1 => '11', :param2 => '23:26', :param3 => '11:36'} }
    let(:partition_3) { {:param1 => '11', :param2 => '24:26', :param3 => '21:54'} }

    before { counter.process(partition_1) }
    before { 2.times { counter.process(partition_2) } }
    before { 3.times { counter.process(partition_3) } }

    it { expect(counter.partitions.first[:param2]).to eq '22:35' }
    it { expect(counter.partitions.second[:param2]).to eq '23:26' }
    it { expect(counter.partitions.third[:param2]).to eq '24:26' }

    it { expect(counter.data.first[:param3]).to eq '11:64' }
    it { expect(counter.data.first[:value]).to eq 1 }
    it { expect(counter.data.second[:param3]).to eq '11:36' }
    it { expect(counter.data.second[:value]).to eq 2 }
    it { expect(counter.data.third[:param3]).to eq '21:54' }
    it { expect(counter.data.third[:value]).to eq 3 }
  end
end
