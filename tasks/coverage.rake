# coding: utf-8

MINIMUM_COVERAGE = 85

desc "Check if test coverage is equal or greater than %.2f%%" % MINIMUM_COVERAGE
task :coverage => :spec do
  require 'simplecov'
  require 'simplecov/exit_codes'

  covered_percent = SimpleCov.result.covered_percent.round(2)
  if covered_percent < MINIMUM_COVERAGE
    $stderr.puts "Coverage (%.2f%%) is below the expected minimum coverage (%.2f%%)." % \
                     [covered_percent, MINIMUM_COVERAGE]

    exit(SimpleCov::ExitCodes::MINIMUM_COVERAGE)
  end
end

task :clean do
  FileUtils.rm_rf 'coverage'
end