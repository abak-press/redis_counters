# coding: utf-8
shared_examples_for 'unique_values_lists' do
  let(:redis) { MockRedis.new }
  let(:values) { rand(10) + 1 }

  let(:options) { {
      :counter_name => :test_counter,
      :value_keys   => [:param0]
  } }

  let(:counter) { described_class.new(redis, options) }

  context '#add' do
    context 'when value_keys not given' do
      let(:options) { {:counter_name => :test_counter} }

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

      before { values.times { counter.add(:param0 => 1, :param1 => 2, :param2 => :group1, :param3 => :part1, :param4 => :part2) } }
      before { values.times { counter.add(:param0 => 2, :param1 => 1, :param2 => :group1, :param3 => :part1, :param4 => :part2) } }
      before { values.times { counter.add(:param0 => 3, :param1 => 2, :param2 => :group1, :param3 => :part2, :param4 => :part2) } }
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :param2 => :group2, :param3 => :part1, :param4 => :part2) } }

      it { expect(redis.keys('*')).to have(5).key }

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

    context 'when group and partition keys no given' do
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

    context 'when no group keys given, but partition keys given' do
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

    context 'when group keys given, but partition keys not given' do
      let(:options) { {
          :counter_name   => :test_counter,
          :value_keys     => [:param0, :param1],
          :group_keys     => [:param2]
      } }

      before { values.times { counter.add(:param0 => 1, :param1 => 2, :param2 => :group1) } }
      before { values.times { counter.add(:param0 => 2, :param1 => 1, :param2 => :group1) } }
      before { values.times { counter.add(:param0 => 3, :param1 => 2, :param2 => :group1) } }
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :param2 => :group2) } }

      it { expect(redis.keys('*')).to have(2).key }

      it { expect(redis.exists("test_counter:group1")).to be_true }
      it { expect(redis.exists("test_counter:group2")).to be_true }

      it { expect(redis.smembers("test_counter:group1")).to have(3).keys }
      it { expect(redis.smembers("test_counter:group2")).to have(1).keys }

      it { expect(redis.smembers("test_counter:group1")).to include '1:2' }
      it { expect(redis.smembers("test_counter:group1")).to include '2:1' }
      it { expect(redis.smembers("test_counter:group1")).to include '3:2' }
      it { expect(redis.smembers("test_counter:group2")).to include '4:5' }
    end

    context 'when block given' do
      let(:options) { {
          :counter_name   => :test_counter,
          :value_keys     => [:param0]
      } }

      context 'when item added' do
        it { expect { |b| counter.add(:param0 => 1, &b) }.to yield_with_args(redis) }
        it { expect(counter.add(:param0 => 1)).to be_true }
      end

      context 'when item not added' do
        before { counter.add(:param0 => 1) }

        it { expect { |b| counter.add(:param0 => 1, &b) }.to_not yield_with_args(redis) }
        it { expect(counter.add(:param0 => 1)).to be_false }
      end
    end
  end

  context '#partitions' do
    let(:group1_subgroup1) { {:group => :group1, :subgroup => :subgroup1} }
    let(:group1_subgroup2) { {:group => :group1, :subgroup => :subgroup2} }
    let(:group1_subgroup3) { {:group => :group1, :subgroup => :subgroup3} }
    let(:group2_subgroup1) { {:group => :group2, :subgroup => :subgroup1} }

    let(:part1_subpart1) { {:part => 'part1', :subpart => 'subpart1'}.with_indifferent_access }
    let(:part1_subpart2) { {:part => 'part1', :subpart => 'subpart2'}.with_indifferent_access }
    let(:part2_subpart1) { {'part' => 'part2', :subpart => 'subpart1'}.with_indifferent_access }

    context 'when group and partition keys given' do
      let(:options) { {
          :counter_name   => :test_counter,
          :value_keys     => [:param0, :param1],
          :group_keys     => [:group, :subgroup],
          :partition_keys => [:part, :subpart]
      } }

      # 2 разных знач в одной группе и партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      # дубль знач в другой партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart3) } }
      # дубль знач в другой группе
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup3, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подпартиции
      before { values.times { counter.add(:param0 => 3, :param1 => 4, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart2) } }
      # новое значение в новой партиции
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :group => :group1, :subgroup => :subgroup1, :part => :part2, :subpart => :subpart1) } }
      # новое значение в новой группе
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :group => :group2, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подгруппе
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :group => :group1, :subgroup => :subgroup2, :part => :part1, :subpart => :subpart1) } }

      context 'when no group given' do
        it { expect { counter.partitions }.to raise_error ArgumentError }
      end

      context 'when no leaf group given' do
        it { expect { counter.partitions(:group => :group1) }.to raise_error KeyError }
      end

      context 'when unknown group given' do
        it { expect(counter.partitions({:group => :unknown_group, :subgroup => :subgroup})).to have(0).partitions }
      end

      context 'when no partition given' do
        it { expect(counter.partitions(group1_subgroup1)).to have(3).partitions }
        it { expect(counter.partitions(group1_subgroup1).first).to eq part1_subpart1 }
        it { expect(counter.partitions(group1_subgroup1).second).to eq part1_subpart2 }
        it { expect(counter.partitions(group1_subgroup1).third).to eq part2_subpart1 }
        #
        it { expect(counter.partitions(group2_subgroup1)).to have(1).partitions }
        it { expect(counter.partitions(group2_subgroup1).first).to eq part1_subpart1 }
      end

      context 'when not leaf partition given' do
        it { expect(counter.partitions(group1_subgroup1, [{:part => :part1}, {:part => :part1}, {:part => :part13}])).to have(2).partitions }
        it { expect(counter.partitions(group1_subgroup1, {:part => :part1}).first).to eq part1_subpart1 }
        it { expect(counter.partitions(group1_subgroup1, {:part => :part1}).second).to eq part1_subpart2 }
      end

      context 'when leaf partition given' do
        it { expect(counter.partitions(group1_subgroup1, {:part => :part1, 'subpart' => 'subpart1'})).to have(1).partitions }
        it { expect(counter.partitions(group1_subgroup1, {:part => :part1, 'subpart' => 'subpart1'}).first).to eq part1_subpart1 }
      end
    end

    context 'when not group keys given and partition keys given' do
      let(:options) { {
          :counter_name   => :test_counter,
          :value_keys     => [:param0, :param1],
          :partition_keys => [:part, :subpart]
      } }

      # 2 разных знач в одной партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :part => :part1, :subpart => :subpart1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :part => :part1, :subpart => :subpart1) } }
      # дубль знач в другой партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :part => :part1, :subpart => :subpart3) } }
      # новое значение в новой подпартиции
      before { values.times { counter.add(:param0 => 3, :param1 => 4, :part => :part1, :subpart => :subpart2) } }
      # новое значение в новой партиции
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :part => :part2, :subpart => :subpart1) } }

      context 'when no group given' do
        it { expect { counter.partitions }.to_not raise_error KeyError }
      end

      context 'when no partition given' do
        it { expect(counter.partitions).to have(3).partitions }
        it { expect(counter.partitions.first).to eq part1_subpart1 }
        it { expect(counter.partitions.second).to eq part1_subpart2 }
        it { expect(counter.partitions.third).to eq part2_subpart1 }
      end

      context 'when not leaf partition given' do
        it { expect(counter.partitions({}, [{:part => :part1}, {:part => :part1}, {:part => :part13}])).to have(2).partitions }
        it { expect(counter.partitions({}, {:part => :part1}).first).to eq part1_subpart1 }
        it { expect(counter.partitions({}, {:part => :part1}).second).to eq part1_subpart2 }
      end

      context 'when leaf partition given' do
        it { expect(counter.partitions({}, {:part => :part1, 'subpart' => 'subpart1'})).to have(1).partitions }
        it { expect(counter.partitions({}, {:part => :part1, 'subpart' => 'subpart1'}).first).to eq part1_subpart1 }
      end
    end

    context 'when group keys given and partition keys not given' do
      let(:options) { {
          :counter_name   => :test_counter,
          :value_keys     => [:param0, :param1],
          :group_keys     => [:group, :subgroup]
      } }

      # 2 разных знач в одной группе
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :group => :group1, :subgroup => :subgroup1) } }
      # дубль знач в другой группе
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup3) } }
      # новое значение в новой группе
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :group => :group2, :subgroup => :subgroup1) } }
      # новое значение в новой подгруппе
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :group => :group1, :subgroup => :subgroup2) } }


      context 'when no partition given' do
        it { expect(counter.partitions(group1_subgroup1)).to have(1).partitions }
        it { expect(counter.partitions(group1_subgroup1).first).to eq Hash.new }

        it { expect(counter.partitions(group2_subgroup1)).to have(1).partitions }
        it { expect(counter.partitions(group2_subgroup1).first).to eq Hash.new }
      end
    end
  end

  context '#data' do
    let(:group1_subgroup1) { {:group => :group1, :subgroup => :subgroup1} }
    let(:group1_subgroup2) { {:group => :group1, :subgroup => :subgroup2} }
    let(:group1_subgroup3) { {:group => :group1, :subgroup => :subgroup3} }
    let(:group2_subgroup1) { {:group => :group2, :subgroup => :subgroup1} }

    let(:part1_subpart1) { {:part => 'part1', :subpart => 'subpart1'}.with_indifferent_access }
    let(:part1_subpart2) { {:part => 'part1', :subpart => 'subpart2'}.with_indifferent_access }
    let(:part2_subpart1) { {'part' => 'part2', :subpart => 'subpart1'}.with_indifferent_access }

    context 'when group and partition keys given' do
      let(:options) { {
          :counter_name   => :test_counter,
          :value_keys     => [:param0, :param1],
          :group_keys     => [:group, :subgroup],
          :partition_keys => [:part, :subpart]
      } }

      # 2 разных знач в одной группе и партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      # дубль знач в другой партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart3) } }
      # дубль знач в другой группе
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup3, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подпартиции
      before { values.times { counter.add(:param0 => 3, :param1 => 4, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart2) } }
      # новое значение в новой партиции
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :group => :group1, :subgroup => :subgroup1, :part => :part2, :subpart => :subpart1) } }
      # новое значение в новой группе
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :group => :group2, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подгруппе
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :group => :group1, :subgroup => :subgroup2, :part => :part1, :subpart => :subpart1) } }

      context 'when no group given' do
        it { expect { counter.data }.to raise_error ArgumentError }
      end

      context 'when no leaf group given' do
        it { expect { counter.data(:group => :group1) }.to raise_error KeyError }
      end

      context 'when unknown group given' do
        it { expect(counter.data({:group => :unknown_group, :subgroup => :subgroup})).to have(0).partitions }
      end

      context 'when no partition given' do
        it { expect(counter.data(group1_subgroup1)).to have(4).rows }
        it { expect(counter.data(group1_subgroup1)).to include ({'param0' => '1', 'param1' => '2'}) }
        it { expect(counter.data(group1_subgroup1)).to include ({'param0' => '1', 'param1' => '3'}) }
        it { expect(counter.data(group1_subgroup1)).to include ({'param0' => '3', 'param1' => '4'}) }
        it { expect(counter.data(group1_subgroup1)).to include ({'param0' => '4', 'param1' => '5'}) }

        it { expect(counter.data(group2_subgroup1)).to have(1).rows }
        it { expect(counter.data(group2_subgroup1).first).to include ({'param0' => '5', 'param1' => '6'}) }
      end

      context 'when not leaf partition given' do
        it { expect(counter.data(group1_subgroup1, [{:part => :part1}, {:part => :part1}, {:part => :part13}])).to have(3).rows }
        it { expect(counter.data(group1_subgroup1, {:part => :part1})).to include ({'param0' => '1', 'param1' => '2'}) }
        it { expect(counter.data(group1_subgroup1, {:part => :part1})).to include ({'param0' => '1', 'param1' => '3'}) }
        it { expect(counter.data(group1_subgroup1, {:part => :part1})).to include ({'param0' => '3', 'param1' => '4'}) }
      end

      context 'when leaf partition given' do
        it { expect(counter.data(group1_subgroup1, {:part => :part1, 'subpart' => 'subpart1'})).to have(2).rows }
        it { expect(counter.data(group1_subgroup1, {:part => :part1, 'subpart' => 'subpart1'})).to include ({'param0' => '1', 'param1' => '2'}) }
        it { expect(counter.data(group1_subgroup1, {:part => :part1, 'subpart' => 'subpart1'})).to include ({'param0' => '1', 'param1' => '3'}) }
      end
    end

    context 'when not group keys given and partition keys given' do
      let(:options) { {
          :counter_name   => :test_counter,
          :value_keys     => [:param0, :param1],
          :partition_keys => [:part, :subpart]
      } }

      # 2 разных знач в одной партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :part => :part1, :subpart => :subpart1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :part => :part1, :subpart => :subpart1) } }
      # дубль знач в другой партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :part => :part1, :subpart => :subpart3) } }
      # новое значение в новой подпартиции
      before { values.times { counter.add(:param0 => 3, :param1 => 4, :part => :part1, :subpart => :subpart2) } }
      # новое значение в новой партиции
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :part => :part2, :subpart => :subpart1) } }

      context 'when no group given' do
        it { expect { counter.data }.to_not raise_error KeyError }
      end

      context 'when no partition given' do
        it { expect(counter.data).to have(4).rows }
        it { expect(counter.data).to include ({'param0' => '1', 'param1' => '2'}) }
        it { expect(counter.data).to include ({'param0' => '1', 'param1' => '3'}) }
        it { expect(counter.data).to include ({'param0' => '3', 'param1' => '4'}) }
        it { expect(counter.data).to include ({'param0' => '4', 'param1' => '5'}) }
      end

      context 'when not leaf partition given' do
        it { expect(counter.data({}, [{:part => :part1}, {:part => :part1}, {:part => :part13}])).to have(3).rows }
        it { expect(counter.data({}, {:part => :part1})).to include ({'param0' => '1', 'param1' => '2'}) }
        it { expect(counter.data({}, {:part => :part1})).to include ({'param0' => '1', 'param1' => '3'}) }
        it { expect(counter.data({}, {:part => :part1})).to include ({'param0' => '3', 'param1' => '4'}) }
      end

      context 'when leaf partition given' do
        it { expect(counter.data({}, {:part => :part1, 'subpart' => 'subpart1'})).to include ({'param0' => '1', 'param1' => '2'}) }
        it { expect(counter.data({}, {:part => :part1, 'subpart' => 'subpart1'}).first).to include ({'param0' => '1', 'param1' => '3'}) }
      end
    end

    context 'when group keys given and partition keys not given' do
      let(:options) { {
          :counter_name   => :test_counter,
          :value_keys     => [:param0, :param1],
          :group_keys     => [:group, :subgroup]
      } }

      # 2 разных знач в одной группе
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :group => :group1, :subgroup => :subgroup1) } }
      # дубль знач в другой группе
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup3) } }
      # новое значение в новой группе
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :group => :group2, :subgroup => :subgroup1) } }
      # новое значение в новой подгруппе
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :group => :group1, :subgroup => :subgroup2) } }


      context 'when no partition given' do
        it { expect(counter.partitions(group1_subgroup1)).to have(1).partitions }
        it { expect(counter.partitions(group1_subgroup1).first).to eq Hash.new }

        it { expect(counter.partitions(group2_subgroup1)).to have(1).partitions }
        it { expect(counter.partitions(group2_subgroup1).first).to eq Hash.new }
      end
    end
  end

  context '#delete_partitions!' do
    let(:group1_subgroup1) { {:group => :group1, :subgroup => :subgroup1} }
    let(:group1_subgroup2) { {:group => :group1, :subgroup => :subgroup2} }
    let(:group1_subgroup3) { {:group => :group1, :subgroup => :subgroup3} }
    let(:group2_subgroup1) { {:group => :group2, :subgroup => :subgroup1} }

    let(:part1_subpart1) { {:part => 'part1', :subpart => 'subpart1'}.with_indifferent_access }
    let(:part1_subpart2) { {:part => 'part1', :subpart => 'subpart2'}.with_indifferent_access }
    let(:part2_subpart1) { {'part' => 'part2', :subpart => 'subpart1'}.with_indifferent_access }

    context 'when group and partition keys given' do
      let(:options) { {
          :counter_name   => :test_counter,
          :value_keys     => [:param0, :param1],
          :group_keys     => [:group, :subgroup],
          :partition_keys => [:part, :subpart]
      } }

      # 2 разных знач в одной группе и партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      # дубль знач в другой партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart3) } }
      # дубль знач в другой группе
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup3, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подпартиции
      before { values.times { counter.add(:param0 => 3, :param1 => 4, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart2) } }
      # новое значение в новой партиции
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :group => :group1, :subgroup => :subgroup1, :part => :part2, :subpart => :subpart1) } }
      # новое значение в новой группе
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :group => :group2, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подгруппе
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :group => :group1, :subgroup => :subgroup2, :part => :part1, :subpart => :subpart1) } }

      context 'when no group given' do
        it { expect { counter.delete_partitions! }.to raise_error ArgumentError }
      end

      context 'when no leaf group given' do
        it { expect { counter.delete_partitions!({:group => :group1}, {:part => 1}) }.to raise_error KeyError }
      end

      context 'when unknown group given' do
        it { expect(counter.delete_partitions!({:group => :unknown_group, :subgroup => :subgroup}, {:part => 1})).to_not raise_error }
      end

      context 'when no partition given' do
        it { expect { counter.delete_partitions!({:group => :group1, :subgroup => :subgroup1}, {}) }.to raise_error ArgumentError }
      end

      context 'when not leaf partition given' do
        before { counter.delete_partitions!(group1_subgroup1, [{:part => :part1}, {:part => :part1}, {:part => :part13}]) }

        it { expect(counter.partitions(group1_subgroup1, :part => :part1)).to have(0).rows }

        it { expect(counter.partitions(group1_subgroup1, :part => :part2)).to be_present }
        it { expect(counter.data(group1_subgroup1, :part => :part2)).to have(1).rows }
        it { expect(counter.data(group1_subgroup1, :part => :part2)).to include ({'param0' => '4', 'param1' => '5'}) }

        it { expect(counter.partitions(group2_subgroup1, :part => :part1)).to be_present }
        it { expect(counter.data(group2_subgroup1, :part => :part1)).to have(1).rows }
        it { expect(counter.data(group2_subgroup1, :part => :part1)).to include ({'param0' => '5', 'param1' => '6'}) }
      end

      context 'when leaf partition given' do
        before { counter.delete_partitions!(group1_subgroup1, {:part => :part1, :subpart => :subpart1}) }

        it { expect(counter.partitions(group1_subgroup1, {:part => :part1, :subpart => :subpart1})).to have(0).rows }

        it { expect(counter.partitions(group1_subgroup1, {:part => :part1, :subpart => :subpart2})).to have(1).rows }
        it { expect(counter.data(group1_subgroup1, {:part => :part1, :subpart => :subpart2})).to include ({'param0' => '3', 'param1' => '4'}) }
      end
    end
  end

  context '#delete_all!' do
    let(:group1_subgroup1) { {:group => :group1, :subgroup => :subgroup1} }
    let(:group1_subgroup2) { {:group => :group1, :subgroup => :subgroup2} }
    let(:group1_subgroup3) { {:group => :group1, :subgroup => :subgroup3} }
    let(:group2_subgroup1) { {:group => :group2, :subgroup => :subgroup1} }

    let(:part1_subpart1) { {:part => 'part1', :subpart => 'subpart1'}.with_indifferent_access }
    let(:part1_subpart2) { {:part => 'part1', :subpart => 'subpart2'}.with_indifferent_access }
    let(:part2_subpart1) { {'part' => 'part2', :subpart => 'subpart1'}.with_indifferent_access }

    context 'when group and partition keys given' do
      let(:options) { {
          :counter_name   => :test_counter,
          :value_keys     => [:param0, :param1],
          :group_keys     => [:group, :subgroup],
          :partition_keys => [:part, :subpart]
      } }

      # 2 разных знач в одной группе и партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      # дубль знач в другой партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart3) } }
      # дубль знач в другой группе
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :group => :group1, :subgroup => :subgroup3, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подпартиции
      before { values.times { counter.add(:param0 => 3, :param1 => 4, :group => :group1, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart2) } }
      # новое значение в новой партиции
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :group => :group1, :subgroup => :subgroup1, :part => :part2, :subpart => :subpart1) } }
      # новое значение в новой группе
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :group => :group2, :subgroup => :subgroup1, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подгруппе
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :group => :group1, :subgroup => :subgroup2, :part => :part1, :subpart => :subpart1) } }

      context 'when no group given' do
        it { expect { counter.delete_all! }.to raise_error ArgumentError }
      end

      context 'when no leaf group given' do
        it { expect { counter.delete_all!(:group => :group1) }.to raise_error KeyError }
      end

      context 'when unknown group given' do
        before { counter.delete_all!({:group => :unknown_group, :subgroup => :subgroup}) }

        it { expect(counter.partitions({:group => :unknown_group, :subgroup => :subgroup})).to have(0).partitions }
      end

      context 'when no partition given' do
        before { counter.delete_all!(group1_subgroup1) }

        it { expect(counter.data(group1_subgroup1)).to have(0).rows }
        it { expect(counter.data(group2_subgroup1)).to have(1).rows }
        it { expect(counter.data(group1_subgroup2)).to have(1).rows }
        it { expect(counter.data(group2_subgroup1)).to include ({'param0' => '5', 'param1' => '6'}) }
        it { expect(counter.data(group1_subgroup2)).to include ({'param0' => '6', 'param1' => '7'}) }

        it { expect(counter.partitions(group1_subgroup1)).to have(0).partitions }
        it { expect(counter.partitions(group2_subgroup1)).to have(1).partitions }
        it { expect(counter.partitions(group2_subgroup1)).to have(1).partitions }
      end
    end
  end
end