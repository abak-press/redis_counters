# coding: utf-8

desc 'Audit current gemset'
task :audit do
  spawn 'bundle exec bundle-audit'
end