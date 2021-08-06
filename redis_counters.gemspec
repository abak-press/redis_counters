lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'redis_counters/version'

Gem::Specification.new do |spec|
  spec.name          = 'redis_counters'
  spec.version       = RedisCounters::VERSION
  spec.authors       = ['Artem Napolskih']
  spec.email         = %w(napolskih@gmail.com)
  spec.summary       = %q{Redis Counters}
  spec.homepage      = 'https://github.com/abak-press/redis_counters'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_runtime_dependency 'activesupport', '>= 4.0', '< 5'

  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'rspec', '~> 2.14.0'
  spec.add_development_dependency 'redis', '>= 3.0'
  spec.add_development_dependency 'appraisal', '>= 1.0.2'
  spec.add_development_dependency 'timecop'
  spec.add_development_dependency 'codeclimate-test-reporter', '>= 0.4.1'
  spec.add_development_dependency 'simplecov'
  spec.add_development_dependency 'cane', '>= 2.6.0'
  spec.add_development_dependency 'bundler-audit'
  spec.add_development_dependency 'pry-byebug'
end
