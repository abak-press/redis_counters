# coding: utf-8
require 'bundler/gem_tasks'

# load everything from tasks/ directory
Dir[File.join(File.dirname(__FILE__), 'tasks', '*.{rb,rake}')].each { |f| load(f) }

task :release => [:check, :changelog]

desc 'Check quality'
task :check => [:audit, :quality, :coverage]

require 'rspec/core/rake_task'

# setup `spec` task
RSpec::Core::RakeTask.new(:spec)