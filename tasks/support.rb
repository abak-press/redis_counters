# coding: utf-8
# Helpers for root Rakefile

require 'pty'

ROOT = File.expand_path(File.join('..', '..'), __FILE__)

# run +cmd+ in subprocess, redirect its stdout to parent's stdout
def spawn(cmd)
  puts ">> #{cmd}"

  cmd += ' 2>&1'
  PTY.spawn cmd do |r, w, pid|
    begin
      r.sync
      r.each_char { |chr| STDOUT.write(chr) }
    rescue Errno::EIO => e
      # simply ignoring this
    ensure
      ::Process.wait pid
    end
  end
  abort "#{cmd} failed" unless $? && $?.exitstatus == 0
end

def project
  'redis_counters'
end

# get current version from VERSION file
def current_version
  File.read(File.join(ROOT, 'VERSION')).strip.chomp
end

# get released version from git
def released_version
  /\Av([\d\.]+)\z/ === `git describe --tags --abbrev=0 2>/dev/null || echo 'v0.0.0'`.chomp.strip

  $1
end