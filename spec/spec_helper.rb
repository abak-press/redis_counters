require 'bundler/setup'
require 'rspec'
require 'timecop'
require 'codeclimate-test-reporter'
require 'simplecov'
require 'pry-byebug'

SimpleCov.start('test_frameworks')

require 'redis_counters'
require 'redis'

Redis.current = Redis.new(host: ENV['TEST_REDIS_HOST'])

# require helpers
support_dir = File.expand_path(File.join('..', 'support'), __FILE__)
Dir[File.join(support_dir, '**', '*.rb')].each { |f| require f }

RSpec.configure do |config|
  config.backtrace_exclusion_patterns = [/lib\/rspec\/(core|expectations|matchers|mocks)/]
  config.before { Redis.current.flushdb }
end
