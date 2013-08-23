# coding: utf-8
namespace :version do
  task :current do
    puts current_version
  end

  desc 'Write a version from VERSION file to project lib/**/version.rb'
  task :update do
    Dir['lib/**/version.rb'].each do |file|
      contents = File.read(file)
      contents.gsub!(/VERSION\s*=\s*(['"])(.*?)\1/m, "VERSION = '#{current_version}'")
      File.write(file, contents)
    end
  end

  desc 'Put version files to repo'
  task :commit do
    Dir['lib/**/version.rb'].each do |file|
      spawn "git add #{file}"
    end
    # git diff --exit-code returns 0 if nothing was changed and 1 otherwise
    spawn "git diff --cached --exit-code > /dev/null || git commit -m \"Release #{current_version}\" || echo -n"
  end

  desc 'Release new version'
  task :release => [:changelog, :update, :commit]

  desc 'Generate CHANGELOG file'
  task :changelog do
    changelog = File.join(ROOT, 'CHANGELOG')

    spawn "changelogger changelog '#{ROOT}' --top_version='v#{current_version}' > '#{changelog}'"
    spawn "git add '#{changelog}'"
  end
end