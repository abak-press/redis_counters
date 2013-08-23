require 'spec_helper'

describe RedisCounters::BaseCounter do
  let(:redis) { MockRedis.new }

  let(:options) { {
    :counter_class => RedisCounters::HashCounter,
    :counter_name  => :counter_name,
    :field_name    => :field_name
  } }

  let(:counter) { described_class.new(redis, options) }

  context '.create' do
    it { expect(described_class.create(redis, options)).to be_a RedisCounters::HashCounter }
  end

  context '#process' do
    it { expect(described_class.create(redis, options)).to respond_to :process }
  end

  context 'when counter_name not given' do
    let(:options) { {
        :field_name => :field_name
    } }

    it { expect { counter }.to raise_error KeyError }
  end
end