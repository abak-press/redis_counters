# coding: utf-8
shared_examples_for 'unique_values_lists/common' do
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

    context 'when unknown cluster_key given' do
      let(:options) { {
        :counter_name => :test_counter,
        :value_keys   => [:param0],
        :cluster_keys   => [:param1, :param2],
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

  context '#has_value?' do
    let(:options) { {
      :counter_name   => :test_counter,
      :value_keys     => [:param0]
    } }

    context 'when item not exists' do
      it { expect(counter.has_value?(:param0 => 1)).to be_false }
    end

    context 'when item exists' do
      before { counter.add(:param0 => 1) }

      it { expect(counter.has_value?(:param0 => 1)).to be_true }
      it { expect(counter.has_value?(:param0 => 2)).to be_false }
    end
  end

  context '#partitions' do
    let(:cluster1_subcluster1) { {:cluster => :cluster1, :subcluster => :subcluster1} }
    let(:cluster1_subcluster2) { {:cluster => :cluster1, :subcluster => :subcluster2} }
    let(:cluster1_subcluster3) { {:cluster => :cluster1, :subcluster => :subcluster3} }
    let(:cluster2_subcluster1) { {:cluster => :cluster2, :subcluster => :subcluster1} }

    let(:part1_subpart1) { {:part => 'part1', :subpart => 'subpart1'}.with_indifferent_access }
    let(:part1_subpart2) { {:part => 'part1', :subpart => 'subpart2'}.with_indifferent_access }
    let(:part2_subpart1) { {'part' => 'part2', :subpart => 'subpart1'}.with_indifferent_access }

    context 'when cluster and partition keys given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :cluster_keys     => [:cluster, :subcluster],
        :partition_keys => [:part, :subpart]
      } }

      # 2 разных знач в одном кластере и партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      # дубль знач в другой партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart3) } }
      # дубль знач в другом кластере
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster3, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подпартиции
      before { values.times { counter.add(:param0 => 3, :param1 => 4, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart2) } }
      # новое значение в новой партиции
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part2, :subpart => :subpart1) } }
      # новое значение в новом кластере
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :cluster => :cluster2, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новом подкластере
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :cluster => :cluster1, :subcluster => :subcluster2, :part => :part1, :subpart => :subpart1) } }

      context 'when no cluster given' do
        it { expect { counter.partitions }.to raise_error ArgumentError }
      end

      context 'when no leaf cluster given' do
        it { expect { counter.partitions(:cluster => :cluster1) }.to raise_error KeyError }
      end

      context 'when unknown cluster given' do
        it { expect(counter.partitions({:cluster => :unknown_cluster, :subcluster => :subcluster})).to have(0).partitions }
      end

      context 'when no partition given' do
        it { expect(counter.partitions(cluster1_subcluster1)).to have(3).partitions }
        it { expect(counter.partitions(cluster1_subcluster1).first).to eq part1_subpart1 }
        it { expect(counter.partitions(cluster1_subcluster1).second).to eq part1_subpart2 }
        it { expect(counter.partitions(cluster1_subcluster1).third).to eq part2_subpart1 }
        #
        it { expect(counter.partitions(cluster2_subcluster1)).to have(1).partitions }
        it { expect(counter.partitions(cluster2_subcluster1).first).to eq part1_subpart1 }
      end

      context 'when not leaf partition given' do
        it { expect(counter.partitions(cluster1_subcluster1.merge(:part => :part1))).to have(2).partitions }
        it { expect(counter.partitions(cluster1_subcluster1.merge(:part => :part1)).first).to eq part1_subpart1 }
        it { expect(counter.partitions(cluster1_subcluster1.merge(:part => :part1)).second).to eq part1_subpart2 }
      end

      context 'when leaf partition given' do
        it { expect(counter.partitions(cluster1_subcluster1.merge(:part => :part1, 'subpart' => 'subpart1'))).to have(1).partitions }
        it { expect(counter.partitions(cluster1_subcluster1.merge(:part => :part1, 'subpart' => 'subpart1')).first).to eq part1_subpart1 }
      end
    end

    context 'when not cluster keys given and partition keys given' do
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

      context 'when no cluster given' do
        it { expect { counter.partitions }.to_not raise_error KeyError }
      end

      context 'when no partition given' do
        it { expect(counter.partitions).to have(3).partitions }
        it { expect(counter.partitions.first).to eq part1_subpart1 }
        it { expect(counter.partitions.second).to eq part1_subpart2 }
        it { expect(counter.partitions.third).to eq part2_subpart1 }
      end

      context 'when not leaf partition given' do
        it { expect(counter.partitions(:part => :part1)).to have(2).partitions }
        it { expect(counter.partitions(:part => :part1).first).to eq part1_subpart1 }
        it { expect(counter.partitions(:part => :part1).second).to eq part1_subpart2 }
      end

      context 'when leaf partition given' do
        it { expect(counter.partitions(:part => :part1, 'subpart' => 'subpart1')).to have(1).partitions }
        it { expect(counter.partitions(:part => :part1, 'subpart' => 'subpart1').first).to eq part1_subpart1 }
      end
    end

    context 'when cluster keys given and partition keys not given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :cluster_keys     => [:cluster, :subcluster]
      } }

      # 2 разных знач в одном кластере
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :cluster => :cluster1, :subcluster => :subcluster1) } }
      # дубль знач в другом кластере
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster3) } }
      # новое значение в новом кластере
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :cluster => :cluster2, :subcluster => :subcluster1) } }
      # новое значение в новом подкластере
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :cluster => :cluster1, :subcluster => :subcluster2) } }


      context 'when no partition given' do
        it { expect(counter.partitions(cluster1_subcluster1)).to have(1).partitions }
        it { expect(counter.partitions(cluster1_subcluster1).first).to eq Hash.new }

        it { expect(counter.partitions(cluster2_subcluster1)).to have(1).partitions }
        it { expect(counter.partitions(cluster2_subcluster1).first).to eq Hash.new }
      end
    end
  end

  context '#data' do
    let(:cluster1_subcluster1) { {:cluster => :cluster1, :subcluster => :subcluster1} }
    let(:cluster1_subcluster2) { {:cluster => :cluster1, :subcluster => :subcluster2} }
    let(:cluster1_subcluster3) { {:cluster => :cluster1, :subcluster => :subcluster3} }
    let(:cluster2_subcluster1) { {:cluster => :cluster2, :subcluster => :subcluster1} }

    let(:part1_subpart1) { {:part => 'part1', :subpart => 'subpart1'}.with_indifferent_access }
    let(:part1_subpart2) { {:part => 'part1', :subpart => 'subpart2'}.with_indifferent_access }
    let(:part2_subpart1) { {'part' => 'part2', :subpart => 'subpart1'}.with_indifferent_access }

    context 'when cluster and partition keys given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :cluster_keys     => [:cluster, :subcluster],
        :partition_keys => [:part, :subpart]
      } }

      # 2 разных знач в одном кластере и партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      # дубль знач в другой партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart3) } }
      # дубль знач в другом кластере
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster3, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подпартиции
      before { values.times { counter.add(:param0 => 3, :param1 => 4, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart2) } }
      # новое значение в новой партиции
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part2, :subpart => :subpart1) } }
      # новое значение в новом кластере
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :cluster => :cluster2, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новом подкластере
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :cluster => :cluster1, :subcluster => :subcluster2, :part => :part1, :subpart => :subpart1) } }

      # context 'when no cluster given' do
      #   it { expect { counter.data }.to raise_error ArgumentError }
      # end
      #
      # context 'when no leaf cluster given' do
      #   it { expect { counter.data(:cluster => :cluster1) }.to raise_error KeyError }
      # end
      #
      # context 'when unknown cluster given' do
      #   it { expect(counter.data(:cluster => :unknown_cluster, :subcluster => :subcluster)).to have(0).partitions }
      # end

      context 'when no partition given' do
        it { expect(counter.data(cluster1_subcluster1)).to have(4).rows }
        it { expect(counter.data(cluster1_subcluster1)).to include ({'param0' => '1', 'param1' => '2'}) }
        it { expect(counter.data(cluster1_subcluster1)).to include ({'param0' => '1', 'param1' => '3'}) }
        it { expect(counter.data(cluster1_subcluster1)).to include ({'param0' => '3', 'param1' => '4'}) }
        it { expect(counter.data(cluster1_subcluster1)).to include ({'param0' => '4', 'param1' => '5'}) }

        it { expect(counter.data(cluster2_subcluster1)).to have(1).rows }
        it { expect(counter.data(cluster2_subcluster1).first).to include ({'param0' => '5', 'param1' => '6'}) }
      end

      context 'when not leaf partition given' do
        it { expect(counter.data(cluster1_subcluster1.merge(:part => :part1))).to have(3).rows }
        it { expect(counter.data(cluster1_subcluster1.merge(:part => :part1))).to include ({'param0' => '1', 'param1' => '2'}) }
        it { expect(counter.data(cluster1_subcluster1.merge(:part => :part1))).to include ({'param0' => '1', 'param1' => '3'}) }
        it { expect(counter.data(cluster1_subcluster1.merge(:part => :part1))).to include ({'param0' => '3', 'param1' => '4'}) }
      end

      context 'when leaf partition given' do
        it { expect(counter.data(cluster1_subcluster1.merge(:part => :part1, 'subpart' => 'subpart1'))).to have(2).rows }
        it { expect(counter.data(cluster1_subcluster1.merge(:part => :part1, 'subpart' => 'subpart1'))).to include ({'param0' => '1', 'param1' => '2'}) }
        it { expect(counter.data(cluster1_subcluster1.merge(:part => :part1, 'subpart' => 'subpart1'))).to include ({'param0' => '1', 'param1' => '3'}) }
      end
    end

    context 'when not cluster keys given and partition keys given' do
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

      context 'when no cluster given' do
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
        it { expect(counter.data(:part => :part1)).to have(3).rows }
        it { expect(counter.data(:part => :part1)).to include ({'param0' => '1', 'param1' => '2'}) }
        it { expect(counter.data(:part => :part1)).to include ({'param0' => '1', 'param1' => '3'}) }
        it { expect(counter.data(:part => :part1)).to include ({'param0' => '3', 'param1' => '4'}) }
      end

      context 'when leaf partition given' do
        it { expect(counter.data(:part => :part1, 'subpart' => 'subpart1')).to have(2).items }
        it { expect(counter.data(:part => :part1, 'subpart' => 'subpart1')).to include ({'param0' => '1', 'param1' => '2'}) }
        it { expect(counter.data(:part => :part1, 'subpart' => 'subpart1')).to include ({'param0' => '1', 'param1' => '3'}) }
      end
    end

    context 'when cluster keys given and partition keys not given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :cluster_keys     => [:cluster, :subcluster]
      } }

      # 2 разных знач в одном кластере
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :cluster => :cluster1, :subcluster => :subcluster1) } }
      # дубль знач в другом кластере
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster3) } }
      # новое значение в новом кластере
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :cluster => :cluster2, :subcluster => :subcluster1) } }
      # новое значение в новом подкластере
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :cluster => :cluster1, :subcluster => :subcluster2) } }


      context 'when no partition given' do
        it { expect(counter.partitions(cluster1_subcluster1)).to have(1).partitions }
        it { expect(counter.partitions(cluster1_subcluster1).first).to eq Hash.new }

        it { expect(counter.partitions(cluster2_subcluster1)).to have(1).partitions }
        it { expect(counter.partitions(cluster2_subcluster1).first).to eq Hash.new }
      end
    end
  end

  context '#delete_partitions!' do
    let(:cluster1_subcluster1) { {:cluster => :cluster1, :subcluster => :subcluster1} }
    let(:cluster1_subcluster2) { {:cluster => :cluster1, :subcluster => :subcluster2} }
    let(:cluster1_subcluster3) { {:cluster => :cluster1, :subcluster => :subcluster3} }
    let(:cluster2_subcluster1) { {:cluster => :cluster2, :subcluster => :subcluster1} }

    let(:part1_subpart1) { {:part => 'part1', :subpart => 'subpart1'}.with_indifferent_access }
    let(:part1_subpart2) { {:part => 'part1', :subpart => 'subpart2'}.with_indifferent_access }
    let(:part2_subpart1) { {'part' => 'part2', :subpart => 'subpart1'}.with_indifferent_access }

    context 'when cluster and partition keys given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :cluster_keys     => [:cluster, :subcluster],
        :partition_keys => [:part, :subpart]
      } }

      # 2 разных знач в одном кластере и партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      # дубль знач в другой партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart3) } }
      # дубль знач в другом кластере
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster3, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подпартиции
      before { values.times { counter.add(:param0 => 3, :param1 => 4, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart2) } }
      # новое значение в новой партиции
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part2, :subpart => :subpart1) } }
      # новое значение в новом кластере
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :cluster => :cluster2, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новом подкластере
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :cluster => :cluster1, :subcluster => :subcluster2, :part => :part1, :subpart => :subpart1) } }

      context 'when no cluster given' do
        it { expect { counter.delete_partitions! }.to raise_error ArgumentError }
      end

      context 'when no leaf cluster given' do
        it { expect { counter.delete_partitions!(:cluster => :cluster1, :part => 1) }.to raise_error KeyError }
      end

      context 'when unknown cluster given' do
        it { expect(counter.delete_partitions!(:cluster => :unknown_cluster, :subcluster => :subcluster, :part => 1)).to_not raise_error }
      end

      context 'when no partition given' do
        before { counter.delete_partitions!(:cluster => :cluster1, :subcluster => :subcluster1) }

        it { expect(counter.data(:cluster => :cluster1, :subcluster => :subcluster1)).to be_empty }
        it { expect(counter.partitions(:cluster => :cluster1, :subcluster => :subcluster1)).to be_empty }

        it { expect(counter.partitions(cluster2_subcluster1.merge(:part => :part1))).to be_present }
        it { expect(counter.data(cluster2_subcluster1.merge(:part => :part1))).to have(1).rows }
        it { expect(counter.data(cluster2_subcluster1.merge(:part => :part1))).to include ({'param0' => '5', 'param1' => '6'}) }
      end

      context 'when not leaf partition given' do
        before { counter.delete_partitions!(cluster1_subcluster1.merge(:part => :part1)) }

        it { expect(counter.partitions(cluster1_subcluster1.merge(:part => :part1))).to have(0).rows }

        it { expect(counter.partitions(cluster1_subcluster1.merge(:part => :part2))).to be_present }
        it { expect(counter.data(cluster1_subcluster1.merge(:part => :part2))).to have(1).rows }
        it { expect(counter.data(cluster1_subcluster1.merge(:part => :part2))).to include ({'param0' => '4', 'param1' => '5'}) }

        it { expect(counter.partitions(cluster2_subcluster1.merge(:part => :part1))).to be_present }
        it { expect(counter.data(cluster2_subcluster1.merge(:part => :part1))).to have(1).rows }
        it { expect(counter.data(cluster2_subcluster1.merge(:part => :part1))).to include ({'param0' => '5', 'param1' => '6'}) }
      end

      context 'when leaf partition given' do
        before { counter.delete_partitions!(cluster1_subcluster1.merge(:part => :part1, :subpart => :subpart1)) }

        it { expect(counter.partitions(cluster1_subcluster1.merge(:part => :part1, :subpart => :subpart1))).to have(0).rows }

        it { expect(counter.partitions(cluster1_subcluster1.merge(:part => :part1, :subpart => :subpart2))).to have(1).rows }
        it { expect(counter.data(cluster1_subcluster1.merge(:part => :part1, :subpart => :subpart2))).to include ({'param0' => '3', 'param1' => '4'}) }
      end
    end
  end

  context '#delete_all!' do
    let(:cluster1_subcluster1) { {:cluster => :cluster1, :subcluster => :subcluster1} }
    let(:cluster1_subcluster2) { {:cluster => :cluster1, :subcluster => :subcluster2} }
    let(:cluster1_subcluster3) { {:cluster => :cluster1, :subcluster => :subcluster} }
    let(:cluster2_subcluster1) { {:cluster => :cluster2, :subcluster => :subcluster1} }

    let(:part1_subpart1) { {:part => 'part1', :subpart => 'subpart1'}.with_indifferent_access }
    let(:part1_subpart2) { {:part => 'part1', :subpart => 'subpart2'}.with_indifferent_access }
    let(:part2_subpart1) { {'part' => 'part2', :subpart => 'subpart1'}.with_indifferent_access }

    context 'when cluster and partition keys given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :cluster_keys   => [:cluster, :subcluster],
        :partition_keys => [:part, :subpart]
      } }

      # 2 разных знач в одном кластере и партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      # дубль знач в другой партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart3) } }
      # дубль знач в другом кластере
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster3, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подпартиции
      before { values.times { counter.add(:param0 => 3, :param1 => 4, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart2) } }
      # новое значение в новой партиции
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part2, :subpart => :subpart1) } }
      # новое значение в новом кластере
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :cluster => :cluster2, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новом подкластере
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :cluster => :cluster1, :subcluster => :subcluster2, :part => :part1, :subpart => :subpart1) } }

      context 'when no cluster given' do
        it { expect { counter.delete_all! }.to raise_error ArgumentError }
      end

      context 'when no leaf cluster given' do
        it { expect { counter.delete_all!(:cluster => :cluster1) }.to raise_error KeyError }
      end

      context 'when unknown cluster given' do
        before { counter.delete_all!(:cluster => :unknown_cluster, :subcluster => :subcluster) }

        it { expect(counter.partitions(cluster1_subcluster1)).to have(3).partitions }
      end

      context 'when unknown params given' do
        it { expect { counter.delete_all!(:cluster1 => :cluster1) }.to raise_error KeyError }
      end

      context 'when no partition given' do
        before { counter.delete_all!(cluster1_subcluster1) }

        it { expect(counter.data(cluster1_subcluster1)).to have(0).rows }
        it { expect(counter.data(cluster2_subcluster1)).to have(1).rows }
        it { expect(counter.data(cluster1_subcluster2)).to have(1).rows }
        it { expect(counter.data(cluster2_subcluster1)).to include ({'param0' => '5', 'param1' => '6'}) }
        it { expect(counter.data(cluster1_subcluster2)).to include ({'param0' => '6', 'param1' => '7'}) }

        it { expect(counter.partitions(cluster1_subcluster1)).to have(0).partitions }
        it { expect(counter.partitions(cluster2_subcluster1)).to have(1).partitions }
        it { expect(counter.partitions(cluster2_subcluster1)).to have(1).partitions }
      end
    end

    context 'when cluster not given and partition keys given' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:param0, :param1],
        :partition_keys => [:part, :subpart]
      } }

      # 2 разных знач в одном кластере и партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      before { values.times { counter.add(:param0 => 1, :param1 => 3, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      # дубль знач в другой партиции
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart3) } }
      # дубль знач в другом кластере
      before { values.times { counter.add(:param0 => 1, :param1 => 2, :cluster => :cluster1, :subcluster => :subcluster3, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новой подпартиции
      before { values.times { counter.add(:param0 => 3, :param1 => 4, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart2) } }
      # новое значение в новой партиции
      before { values.times { counter.add(:param0 => 4, :param1 => 5, :cluster => :cluster1, :subcluster => :subcluster1, :part => :part2, :subpart => :subpart1) } }
      # новое значение в новом кластере
      before { values.times { counter.add(:param0 => 5, :param1 => 6, :cluster => :cluster2, :subcluster => :subcluster1, :part => :part1, :subpart => :subpart1) } }
      # новое значение в новом подкластере
      before { values.times { counter.add(:param0 => 6, :param1 => 7, :cluster => :cluster1, :subcluster => :subcluster2, :part => :part1, :subpart => :subpart1) } }

      context 'when no cluster given' do
        before { counter.delete_all! }

        it { expect(counter.partitions).to have(0).partitions }
      end

      context 'when unknown cluster given' do
        before { counter.delete_all!(:cluster => :unknown_cluster, :subcluster => :subcluster) }

        it { expect(counter.partitions).to have(0).partitions }
      end

      context 'when unknown params given' do
        before { counter.delete_all!(:cluster1 => :cluster1) }

        it { expect(counter.partitions).to have(0).partitions }
      end
    end
  end
end