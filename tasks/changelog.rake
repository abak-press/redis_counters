desc 'Generate CHANGELOG.md'
task :changelog do
  require 'apress/changelogger'
  Apress::ChangeLogger.new.log_changes
  spawn "git add CHANGELOG.md"
end
