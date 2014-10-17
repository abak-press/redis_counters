# coding: utf-8
shared_examples_for 'unique_values_lists/expirable' do
  let(:redis) { MockRedis.new }
  let(:counter) { described_class.new(redis, options) }

  after do
    Timecop.return
  end

  context 'when auto expire enabled' do
    context 'when expire given in counter options' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:value],
        :partition_keys => [:part],
        :expire         => 10.seconds
      } }

      before do
        counter.add(:value => 1, :part => :part1)
        counter.add(:value => 2, :part => :part2)
        counter.add(:value => 3, :part => :part1, :expire => :never)
        counter.add(:value => 4, :part => :part1, :expire => 20.seconds)
      end

      context 'before time has expired' do
        before { Timecop.freeze(5.seconds.since) }

        it { expect(counter.has_value?(:value => 1)).to be_true }
        it { expect(counter.has_value?(:value => 2)).to be_true }
        it { expect(counter.has_value?(:value => 3)).to be_true }
        it { expect(counter.has_value?(:value => 4)).to be_true }

        it { expect(counter.partitions).to include('part' => 'part1') }
        it { expect(counter.partitions).to include('part' => 'part2') }

        it { expect(counter.data).to include('value' => '1') }
        it { expect(counter.data).to include('value' => '2') }
        it { expect(counter.data).to include('value' => '3') }
        it { expect(counter.data).to include('value' => '4') }
      end

      context 'after time has expired' do
        before { Timecop.freeze(10.seconds.since) }

        it { expect(counter.has_value?(:value => 1)).to be_false }
        it { expect(counter.has_value?(:value => 2)).to be_false }
        it { expect(counter.has_value?(:value => 3)).to be_true }

        it { expect(counter.partitions).to include('part' => 'part1') }
        it { expect(counter.partitions).to_not include('part' => 'part2') }

        it { expect(counter.data).to_not include('value' => '1') }
        it { expect(counter.data).to_not include('value' => '2') }
        it { expect(counter.data).to include('value' => '3') }
        it { expect(counter.data).to include('value' => '4') }
      end
    end

    context 'when expire not given in counter options' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:value],
        :partition_keys => [:part]
      } }

      before do
        counter.add(:value => 1, :part => :part1, :expire => 10.seconds)
        counter.add(:value => 2, :part => :part2, :expire => 10.seconds)
        counter.add(:value => 3, :part => :part1)
        counter.add(:value => 4, :part => :part1, :expire => 20.seconds)
      end

      context 'before time has expired' do
        before { Timecop.freeze(5.seconds.since) }

        it { expect(counter.has_value?(:value => 1)).to be_true }
        it { expect(counter.has_value?(:value => 2)).to be_true }
        it { expect(counter.has_value?(:value => 3)).to be_true }
        it { expect(counter.has_value?(:value => 4)).to be_true }

        it { expect(counter.partitions).to include('part' => 'part1') }
        it { expect(counter.partitions).to include('part' => 'part2') }

        it { expect(counter.data).to include('value' => '1') }
        it { expect(counter.data).to include('value' => '2') }
        it { expect(counter.data).to include('value' => '3') }
        it { expect(counter.data).to include('value' => '4') }
      end

      context 'after time has expired' do
        before { Timecop.freeze(10.seconds.since) }

        it { expect(counter.has_value?(:value => 1)).to be_false }
        it { expect(counter.has_value?(:value => 2)).to be_false }
        it { expect(counter.has_value?(:value => 3)).to be_true }
        it { expect(counter.has_value?(:value => 4)).to be_true }

        it { expect(counter.partitions).to include('part' => 'part1') }
        it { expect(counter.partitions).to_not include('part' => 'part2') }

        it { expect(counter.data).to_not include('value' => '1') }
        it { expect(counter.data).to_not include('value' => '2') }
        it { expect(counter.data).to include('value' => '3') }
        it { expect(counter.data).to include('value' => '4') }
      end
    end
  end

  context 'when auto expire disabled' do
    context 'when expire given in counter options' do
      let(:options) { {
        :counter_name   => :test_counter,
        :value_keys     => [:value],
        :partition_keys => [:part],
        :expire         => 10.seconds,
        :clean_expired  => false
      } }

      before do
        counter.add(:value => 1, :part => :part1)
        counter.add(:value => 2, :part => :part2)
        counter.add(:value => 3, :part => :part1, :expire => :never)
        counter.add(:value => 4, :part => :part1, :expire => 20.seconds)

        Timecop.freeze(10.seconds.since)
      end

      context 'after time has expired' do
        it { expect(counter.has_value?(:value => 1)).to be_true }
        it { expect(counter.has_value?(:value => 2)).to be_true }
        it { expect(counter.has_value?(:value => 3)).to be_true }
        it { expect(counter.has_value?(:value => 4)).to be_true }

        it { expect(counter.partitions).to include('part' => 'part1') }
        it { expect(counter.partitions).to include('part' => 'part2') }

        it { expect(counter.data).to include('value' => '1') }
        it { expect(counter.data).to include('value' => '2') }
        it { expect(counter.data).to include('value' => '3') }
        it { expect(counter.data).to include('value' => '4') }
      end

      context 'when clean_expired call' do
        before { counter.clean_expired }

        it { expect(counter.has_value?(:value => 1)).to be_false }
        it { expect(counter.has_value?(:value => 2)).to be_false }
        it { expect(counter.has_value?(:value => 3)).to be_true }
        it { expect(counter.has_value?(:value => 4)).to be_true }

        it { expect(counter.partitions).to include('part' => 'part1') }
        it { expect(counter.partitions).to_not include('part' => 'part2') }

        it { expect(counter.data).to_not include('value' => '1') }
        it { expect(counter.data).to_not include('value' => '2') }
        it { expect(counter.data).to include('value' => '3') }
        it { expect(counter.data).to include('value' => '4') }
      end
    end
  end
end