# coding: utf-8

require 'cane/rake_task'

desc 'Run cane to check quality metrics'
Cane::RakeTask.new(:quality) do |cane|
  cane.abc_max = 15
  cane.abc_glob = cane.style_glob = cane.doc_glob = '*/{lib,bin}/**/*.rb'
  cane.style_measure = 120
  cane.parallel = false
  cane.no_doc = true
end