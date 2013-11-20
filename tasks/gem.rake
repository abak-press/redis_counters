# coding: utf-8

desc 'Release gem (build and upload to gem repo)'
task :release => [
  :ensure_master,
  :check,
  'version:release',
  :build,
  :tag,
  :push,
  :upload
]

desc 'Build project into pkg directory'
task :build do
  FileUtils.mkdir_p('pkg')

  gemspec = "#{project}.gemspec"
  spawn("gem build -V #{gemspec}")
  built_gem_path = Dir["#{project}-*.gem"].sort_by{|f| File.mtime(f)}.last

  FileUtils.mv(built_gem_path, 'pkg')
end

desc 'Mark project as stable with version tag'
task :tag do
  tag_name = "v#{current_version}"

  spawn("git tag -a -m \"Version #{current_version}\" #{tag_name}")
  puts "Tag #{tag_name} created"
end

task :push do
  spawn 'git push'
  spawn 'git push --tags'
end

# upload built tarballs to repo
task :upload do
  require 'uri'
  require 'net/http/post/multipart'

  repo = gems_sources.grep(/railsc/).first
  uri = URI.parse(repo)

  tarball_name = "#{project}-#{current_version}.gem"
  upload_gem(uri.dup, tarball_name)
end

task :ensure_master do
  `git rev-parse --abbrev-ref HEAD`.chomp.strip == 'master' || abort("Can be released only from `master` branch")
end

def upload_gem(repo_uri, tarball_name)
  require 'net/http/post/multipart'
  repo_uri.path = '/upload'

  tarball_path = File.join('pkg', tarball_name)

  File.open(tarball_path) do |gem|
    req = Net::HTTP::Post::Multipart.new repo_uri.path,
                                         "file" => UploadIO.new(gem, "application/x-tar", tarball_name)

    req.basic_auth(repo_uri.user, repo_uri.password) if repo_uri.user

    res = Net::HTTP.start(repo_uri.host, repo_uri.port) do |http|
      http.request(req)
    end

    if [200, 302].include?(res.code.to_i)
      puts "#{tarball_name} uploaded successfully"
    else
      $stderr.puts "Cannot upload #{tarball_name}. Response status: #{res.code}"
      exit(1)
    end
  end # File.open
end

task :clean do
  FileUtils.rm_f 'Gemfile.lock'
end

def gems_sources
  Bundler.
      setup. # get bundler runtime
      specs. # for each spec
      map(&:source). # get its sources
      select { |v| Bundler::Source::Rubygems === v }. # fetch only rubygems-like repos
      map(&:remotes). # get all remotes
      flatten.
      uniq.
      map(&:to_s)
end