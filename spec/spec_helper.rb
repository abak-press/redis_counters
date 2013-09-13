require 'rubygems'
require 'bundler/setup'
require 'rspec'
require 'simplecov'
require 'mock_redis'

SimpleCov.start('test_frameworks')

require 'redis_counters'

# require helpers
support_dir = File.expand_path(File.join('..', 'support'), __FILE__)
Dir[File.join(support_dir, '**', '*.rb')].each { |f| require f }

RSpec.configure do |config|
  config.backtrace_exclusion_patterns = [/lib\/rspec\/(core|expectations|matchers|mocks)/]
  config.color_enabled = true
  config.formatter = 'documentation'
  config.order = 'random'
end