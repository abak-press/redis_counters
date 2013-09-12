# coding: utf-8
# load everything from tasks/ directory
Dir[File.join(File.dirname(__FILE__), 'tasks', '*.{rb,rake}')].each { |f| load(f) }

task :build => [:check]
task :tag => :build

desc 'Check if all projects are ready for build process'
task :check => [:audit, :quality, :coverage]

require 'rspec/core/rake_task'

# setup `spec` task
RSpec::Core::RakeTask.new(:spec)